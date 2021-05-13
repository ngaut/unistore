package s3util

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/badger/y"
)

type Options struct {
	InstanceID uint32
	EndPoint   string
	KeyID      string
	SecretKey  string
	Bucket     string
	Region     string
}

type S3Client struct {
	Options
	*scheduler
	cli *s3.S3
}

func NewS3Client(opts Options) *S3Client {
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
	y.NumBytesReadS3.Add(float64(len(result)))
	y.NumGetsS3.Inc()
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
	written, err := io.Copy(fd, out.Body)
	if err != nil {
		return err
	}
	y.NumBytesReadS3.Add(float64(written))
	y.NumGetsS3.Inc()
	return nil
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

func (c *S3Client) IndexKey(fid uint64) string {
	return fmt.Sprintf("bg%08x%016x.idx", c.InstanceID, fid)
}
