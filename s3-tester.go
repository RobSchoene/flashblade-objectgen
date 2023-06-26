package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type S3Tester struct {
	endpoint        string
	accessKey       string
	secretKey       string
	bucket          string
	coreCount		int
	numObjects		int
	prefixLength	int

	atm_finished              int32
	atm_counter_bytes_written uint64
	objectsWritten 		      int32
}

func NewS3Tester(endpoint string, accessKey string, secretKey string, bucketname string, coreCount int, numObjects int, prefixLength int) (*S3Tester, error) {

	s3Tester := &S3Tester{endpoint: endpoint, accessKey: accessKey, secretKey: secretKey, bucket: bucketname, coreCount: coreCount, numObjects: numObjects, prefixLength: prefixLength, objectsWritten: 0}

	sess := s3Tester.newSession()
	svc := s3.New(sess)

	count := 0
	err := svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: &bucketname,
	}, func(p *s3.ListObjectsOutput, _ bool) (shouldContinue bool) {
		count += len(p.Contents)
		return true
	})
	if err != nil {
		fmt.Println("failed to list objects", err)
		return nil, err
	}

	return s3Tester, err
}

func (s *S3Tester) newSession() *session.Session {
	s3Config := &aws.Config{
		Endpoint:         aws.String(s.endpoint),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	if s.accessKey != "" {
		s3Config.Credentials = credentials.NewStaticCredentials(s.accessKey, s.secretKey, "")
	}

	return session.Must(session.NewSession(s3Config))
}

func (s *S3Tester) writeOneObject(id int, jobs <-chan int, results chan<- int) {
	for j := range jobs {
		src := make([]byte, 8*1024)
		rand.Read(src)
		r := bytes.NewReader(src)

		prefix := generateTestObjectName(s.prefixLength, s.objectsWritten)

		sess := s.newSession()
		svc := s3manager.NewUploader(sess)

		bytes_written := uint64(0)

		_, err := svc.Upload(&s3manager.UploadInput{
			Bucket: &s.bucket,
			Key:    &prefix,
			Body:   r,
		})
		if err != nil {
			fmt.Println("error", err)
		}
		bytes_written += uint64(len(src))
		
		atomic.AddInt32(&s.objectsWritten, 1)
		atomic.AddUint64(&s.atm_counter_bytes_written, bytes_written)
		
		// sends the result to the results channel
		results <- j * 2
	}
}

func generateTestObjectName(prefixlen int, i int32) string {

	ret := make([]byte, prefixlen)
	for j := 0; j < prefixlen; j++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		ret[j] = charset[num.Int64()]
	}

	return fmt.Sprintf("StandardPrefix-%s-%d", ret, i)
}

func (s *S3Tester) WriteTest() int32 {
	atomic.StoreInt32(&s.atm_finished, 0)
	atomic.StoreUint64(&s.atm_counter_bytes_written, 0)

	s.objectsWritten = 0
	jobs := make(chan int, s.numObjects)
	results := make(chan int, s.numObjects)
	
	// create goroutines for the writer function
	for w := 1; w <= s.coreCount; w++ {
		go s.writeOneObject(w, jobs, results)
	}

	// send number of objects to the jobs channel
	for j := 1; j <= s.numObjects; j++ {
		jobs <- j
	}
	// close the jobs channel to signal that all jobs have been sent
	close(jobs)

	// receive number of objects from the results channel
	for a := 1; a <= s.numObjects; a++ {
		<-results
	}
	// close the results channel to signal that all results received
	close(results)

	atomic.StoreInt32(&s.atm_finished, 1)

	return s.objectsWritten
}