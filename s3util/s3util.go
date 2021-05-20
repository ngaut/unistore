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
	"sync"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
)

const (
	DeletionFileName = "DELETION"
	deletionSize     = int(unsafe.Sizeof(deletion{}))
)

type Options struct {
	InstanceID  uint32
	EndPoint    string
	KeyID       string
	SecretKey   string
	Bucket      string
	Region      string
	DelayedTime uint64
}

type S3Client struct {
	Options
	*scheduler
	cli       *s3.S3
	dirPath   string
	lock      sync.RWMutex
	deletions deletions
}

func NewS3Client(c *y.Closer, dirPath string, opts Options) *S3Client {
	s3c := &S3Client{
		Options:   opts,
		scheduler: newScheduler(256),
		dirPath:   dirPath,
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
	c.AddRunning(1)
	go s3c.deleteLoop(c)
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
	input := &s3.PutObjectInput{}
	input.SetContentLength(int64(len(data)))
	input.Bucket = &c.Bucket
	input.Key = &key
	input.Body = bytes.NewReader(data)
	_, err := c.cli.PutObject(input)
	return err
}

func (c *S3Client) Delete(key string) error {
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

func (c *S3Client) SetExpiredTime(fid uint64) {
	c.lock.Lock()
	c.deletions = append(c.deletions, deletion{fid: fid, expiredTime: uint64(time.Now().Unix()) + c.DelayedTime})
	c.lock.Unlock()
}

func (c *S3Client) deleteLoop(closer *y.Closer) {
	defer closer.Done()
	ticker := time.NewTicker(time.Second)
	filePath, err := filepath.Abs(filepath.Join(c.dirPath, DeletionFileName))
	if err != nil {
		log.S().Errorf("cannot get absolute path for deletion file: %s", err.Error())
		return
	}
	err = c.deletions.load(filePath)
	if err != nil {
		log.S().Errorf("cannot open deletion file: %s", err.Error())
	}
	for {
		select {
		case <-ticker.C:
			c.deleteExpiredFile()
		case <-closer.HasBeenClosed():
			// write deletions to disk
			err = c.deletions.write(filePath)
			if err != nil {
				log.S().Errorf("cannot write deletion file: %s", err.Error())
			}
			return
		}
	}
}
func (c *S3Client) deleteExpiredFile() {
	c.lock.RLock()
	for len(c.deletions) > 0 {
		f := c.deletions[0]
		if f.expiredTime < uint64(time.Now().Unix()) {
			c.lock.RUnlock()
			c.lock.Lock()
			c.deletions = c.deletions[1:]
			c.lock.Unlock()
			c.Delete(c.BlockKey(f.fid))
			c.lock.RLock()
		} else {
			break
		}
	}
	c.lock.RUnlock()
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
	f.Close()
	f.Sync()
	return os.Rename(tmpFilePath, filePath)
}

func (d *deletions) load(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
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
