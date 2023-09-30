package main

import (
	// "bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	// "time"

	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	// "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

// AWSConfig holds the AWS configuration.
var AWSConfig = aws.Config{
	Region:      aws.String(os.Getenv("AWS_REGION")), // Specify your AWS region in env variable
	Credentials: credentials.NewEnvCredentials(),
}

// The name of the S3 bucket
const S3BucketName = "box-poc-processed-data" // Change this to your S3 bucket name

// Structure of the file record
type FileRecord struct {
	TotalCount int `json:"total_count"`
	Entries    []struct {
		Type             string `json:"type"`
		ID               string `json:"id"`
		Etag             string `json:"etag"`
		Name             string `json:"name"`
		DownloadURL      string `json:"download_url"`
		ContentCreatedAt string `json:"content_created_at"`
		OwnedBy          struct {
			Type  string `json:"type"`
			ID    string `json:"id"`
			Name  string `json:"name"`
			Login string `json:"login"`
		} `json:"owned_by"`
	} `json:"entries"`
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
	Order  []struct {
		By        string `json:"by"`
		Direction string `json:"direction"`
	} `json:"order"`
}

type Files_data struct {
	Type             string    `json:"type"`
	ID               string    `json:"id"`
	Etag             string    `json:"etag"`
	Name             string    `json:"name"`
	DownloadURL      string    `json:"download_url"`
	ContentCreatedAt string    `json:"content_created_at"`
	OwnedBy          struct {
		Type  string `json:"type"`
		ID    string `json:"id"`
		Name  string `json:"name"`
		Login string `json:"login"`
	} `json:"owned_by"`
}

// Function to download a file from the given URL and save it locally
func downloadFile(url, localFilename string) error {
	response, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download file: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download file, status code: %d", response.StatusCode)
	}

	file, err := os.Create(localFilename)
	if err != nil {
		return fmt.Errorf("failed to create local file: %v", err)
	}
	defer file.Close()

	_, err = io.Copy(file, response.Body)
	if err != nil {
		return fmt.Errorf("failed to write file content: %v", err)
	}

	return nil
}

// Function to upload a local file to Amazon S3 using multipart upload
func uploadToS3(localFilename, objectKey string) error {
	sess, err := session.NewSession(&AWSConfig)
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %v", err)
	}

	uploader := s3manager.NewUploader(sess)

	file, err := os.Open(localFilename)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Upload to AWS S3
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(S3BucketName),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %v", err)
	}

	return nil
}

// Function to process the Kinesis records.
func processRecords(request events.KinesisEvent) (events.KinesisEvent, error) {
	fmt.Println(">> Function: START")
	fmt.Printf(">> No of Records: %d\n", len(request.Records))

	var output events.KinesisEvent

	for _, record := range request.Records {
		payload, err := base64.StdEncoding.DecodeString(string(record.Kinesis.Data))
		if err != nil {
			fmt.Printf("Error decoding payload: %v\n", err)
			continue
		}

		// Unmarshalling Event Payload
		var deserializePayload FileRecord
		if err := json.Unmarshal(payload, &deserializePayload); err != nil {
			fmt.Printf("Error unmarshalling payload: %v\n", err)
			continue
		}

		fmt.Printf(">> DeserializePayload: %+v\n", deserializePayload)

		for _, fileRecord := range deserializePayload.Entries {
			fmt.Printf("------------")
			fmt.Printf("=======", reflect.TypeOf(fileRecord))
			// processFileRecord(fileRecord)
		}

	}

	fmt.Printf(">> Successfully processed %d records.\n", len(request.Records))
	fmt.Println(">> Function: COMPLETE")
	return output, nil
}

// Function to process a single file record
func processFileRecord(fileRecord Files_data) {
	fmt.Println(fileRecord.Type)
	if fileRecord.Type == "file" {
		fmt.Println(fileRecord.DownloadURL)
		fmt.Printf("%+v\n", fileRecord)
		fmt.Printf(">> File Record Type: %T\n", fileRecord)
		fmt.Println(fileRecord.ContentCreatedAt)

		contentTimestamp := fileRecord.ContentCreatedAt
		timeSplitArr := strings.Split(contentTimestamp, "-")
		timeSplitArr[2] = timeSplitArr[2][:1]
		s3ObjectKey := strings.Join(timeSplitArr, "/") + "/" + fileRecord.Name

		if err := downloadFile(fileRecord.DownloadURL, fileRecord.Name); err != nil {
			fmt.Printf("Error downloading file: %v\n", err)
			return
		}
		fmt.Println("---File Downloaded to S3:", fileRecord.Name)

		if err := uploadToS3(fileRecord.Name, s3ObjectKey); err != nil {
			fmt.Printf("Error uploading file to S3: %v\n", err)
			return
		}
	}
}

func main() {
	lambda.Start(processRecords)
}
