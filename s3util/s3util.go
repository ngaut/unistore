package s3util

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ngaut/unistore/config"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

const (
	DeletionFileName = "DELETION"
	deletionSize     = int(unsafe.Sizeof(deletion{}))
)

type Options struct {
	InstanceID         uint32
	EndPoint           string
	KeyID              string
	SecretKey          string
	Bucket             string
	Region             string
	ExpirationDuration string
}

type S3Client struct {
	Options
	*scheduler
	cli               *s3.S3
	expirationSeconds uint64
	lock              sync.RWMutex
	deletions         deletions
}

func NewS3Client(c *y.Closer, dirPath string, opts Options) *S3Client {
	s3c := &S3Client{
		Options:   opts,
		scheduler: newScheduler(256),
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{Transport: tr}
	cred := credentials.NewStaticCredentials(opts.KeyID, opts.SecretKey, "")
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithEndpoint(opts.EndPoint).
		WithDisableSSL(true).
		WithS3ForcePathStyle(true).
		WithCredentials(cred).
		WithHTTPClient(client).
		WithRegion(opts.Region)))
	s3c.cli = s3.New(sess)
	if len(opts.ExpirationDuration) > 0 {
		s3c.expirationSeconds = uint64(config.ParseDuration(opts.ExpirationDuration) / time.Second)
		if s3c.expirationSeconds > 0 {
			filePath := filepath.Join(dirPath, DeletionFileName)
			err := s3c.deletions.load(filePath)
			if err != nil {
				log.S().Errorf("cannot open deletion file: %s", err.Error())
			}
			c.AddRunning(1)
			go s3c.deleteLoop(c, filePath)
		}
	}
	return s3c
}

func (c *S3Client) Get(key string, offset, length int64) ([]byte, error) {
	input := &s3.GetObjectInput{}
	input.Bucket = &c.Bucket
	input.Key = &key
	if length > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}
	out, err := c.cli.GetObject(input)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	var result []byte
	if length > 0 {
		result = make([]byte, length)
	} else {
		result = make([]byte, *out.ContentLength)
	}
	_, err = io.ReadFull(out.Body, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *S3Client) GetToFile(key string, filePath string) error {
	fid, ok := c.ParseFileID(key)
	if !ok {
		return errors.New("fail to parse file id:" + key)
	}
	log.S().Infof("get file from s3:%d", fid)
	fd, err := y.OpenTruncFile(filePath, false)
	if err != nil {
		return err
	}
	defer fd.Close()
	input := &s3.GetObjectInput{}
	input.Bucket = &c.Bucket
	input.Key = &key
	out, err := c.cli.GetObject(input)
	if err != nil {
		return err
	}
	defer out.Body.Close()
	_, err = io.Copy(fd, out.Body)
	return err
}

func (c *S3Client) Put(key string, data []byte) error {
	fid, ok := c.ParseFileID(key)
	if !ok {
		return errors.New("fail to parse file id:" + key)
	}
	log.S().Infof("put file to s3:%d", fid)
	input := &s3.PutObjectInput{}
	input.SetContentLength(int64(len(data)))
	input.Bucket = &c.Bucket
	input.Key = &key
	input.Body = bytes.NewReader(data)
	_, err := c.cli.PutObject(input)
	return err
}

func (c *S3Client) Delete(key string) error {
	fid, ok := c.ParseFileID(key)
	if !ok {
		return errors.New("fail to parse file id:" + key)
	}
	log.S().Infof("delete file from s3:%d", fid)
	input := &s3.DeleteObjectInput{}
	input.Bucket = &c.Bucket
	input.Key = &key
	_, err := c.cli.DeleteObject(input)
	return err
}

func (c *S3Client) ListFiles() (map[uint64]struct{}, error) {
	var marker *string
	fileIDs := map[uint64]struct{}{}
	for {
		input := &s3.ListObjectsInput{}
		input.Bucket = &c.Bucket
		input.Prefix = aws.String(fmt.Sprintf("bg%08x", c.InstanceID))
		input.Marker = marker
		output, err := c.cli.ListObjects(input)
		if err != nil {
			return nil, err
		}
		for _, objInfo := range output.Contents {
			var fid uint64
			_, err = fmt.Sscanf((*objInfo.Key)[10:26], "%016x", &fid)
			if err != nil {
				return nil, err
			}
			fileIDs[fid] = struct{}{}
		}
		if !*output.IsTruncated {
			break
		}
		marker = output.NextMarker
	}
	return fileIDs, nil
}

func (c *S3Client) BlockKey(fid uint64) string {
	return fmt.Sprintf("bg%08x%016x.sst", c.InstanceID, fid)
}

func (c *S3Client) ParseFileID(key string) (uint64, bool) {
	if len(key) != 30 {
		return 0, false
	}
	if !strings.HasPrefix(key, "bg") {
		return 0, false
	}
	if !strings.HasSuffix(key, ".sst") {
		return 0, false
	}
	var fid uint64
	_, err := fmt.Sscanf(key[10:26], "%016x", &fid)
	if err != nil {
		return 0, false
	}
	return fid, true
}

func (c *S3Client) SetExpired(fid uint64) {
	if c.expirationSeconds > 0 {
		c.lock.Lock()
		c.deletions = append(c.deletions, deletion{fid: fid, expiredTime: uint64(time.Now().Unix()) + c.expirationSeconds})
		c.lock.Unlock()
	}
}

func (c *S3Client) deleteLoop(closer *y.Closer, filePath string) {
	defer closer.Done()
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			c.deleteExpiredFile()
		case <-closer.HasBeenClosed():
			// write deletions to disk
			err := c.deletions.write(filePath)
			if err != nil {
				log.S().Errorf("cannot write deletion file: %s", err.Error())
			}
			return
		}
	}
}
func (c *S3Client) deleteExpiredFile() {
	var toDelete deletions
	now := uint64(time.Now().Unix())
	c.lock.RLock()
	for i, d := range c.deletions {
		if d.expiredTime > now {
			toDelete = c.deletions[:i]
			break
		} else if i == len(c.deletions)-1 {
			toDelete = c.deletions
		}
	}
	c.lock.RUnlock()
	if len(toDelete) > 0 {
		for _, d := range toDelete {
			if err := c.Delete(c.BlockKey(d.fid)); err != nil {
				log.S().Errorf("cannot delete expired file: %s", err.Error())
			}
		}
		c.lock.Lock()
		c.deletions = c.deletions[len(toDelete):]
		c.lock.Unlock()
	}
}

type deletion struct {
	fid         uint64
	expiredTime uint64
}

type deletions []deletion

func (d deletions) write(filePath string) error {
	tmpFilePath := filePath + ".tmp"
	f, err := os.Create(tmpFilePath)
	if err != nil {
		return err
	}
	f.Write(d.marshal())
	f.Sync()
	f.Close()
	return os.Rename(tmpFilePath, filePath)
}

func (d *deletions) load(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	d.unmarshal(b)
	return nil
}

func (d deletions) marshal() []byte {
	if len(d) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(d) * deletionSize
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&d[0]))
	return b
}

func (d *deletions) unmarshal(b []byte) {
	if len(b) == 0 {
		return
	}
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(d))
	hdr.Len = len(b) / deletionSize
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
}
