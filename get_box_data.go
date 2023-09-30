package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Structure of the JSON data
type BoxResponse struct {
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

// Function to get access token from box.com
func getAccessToken(clientID, clientSecret string) (string, error) {
	url := "https://api.box.com/oauth2/token"
	payload := fmt.Sprintf("grant_type=client_credentials&client_id=%s&client_secret=%s&box_subject_type=user&box_subject_id=28998615808", clientID, clientSecret)

	resp, err := http.Post(url, "application/x-www-form-urlencoded", strings.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("failed to get access token: %v", err)
	}
	defer resp.Body.Close()

	var response struct {
		AccessToken string `json:"access_token"`
	}
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return "", fmt.Errorf("failed to decode access token response: %v", err)
	}

	return response.AccessToken, nil
}

// Function to get data after getting token
func getData(token string) (*BoxResponse, error) {
	url := "https://api.box.com/2.0/folders/0/items?fields=id%2Ctype%2Cname%2Cdownload_url%2Ccontent_created_at%2C%20created_at%2Cowned_by%2C%20modified_by%2C%20owned_by%2C%20created_by"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	authorizationBearer := "Bearer "
	req.Header.Add("Authorization", authorizationBearer+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get data: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-OK status code: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var boxResponse BoxResponse

	err = json.Unmarshal(body, &boxResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	return &boxResponse, nil
}

// Function to publish data to Kinesis
func publishToKinesis(kinesisClient *kinesis.Kinesis, dataRecord interface{}) error {
	data, err := json.Marshal(dataRecord)
	if err != nil {
		return fmt.Errorf("failed to marshal data record: %v", err)
	}
	streamName := os.Getenv("KINESIS_STREAM_NAME")
	partitionKey := os.Getenv("PARTITION_KEY")
	_, err = kinesisClient.PutRecord(&kinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		Data:         data,
		PartitionKey: aws.String(partitionKey),
	})

	return err
}

// Lambda handler
func handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	clientID := os.Getenv("CLIENT_ID")
	clientSecret := os.Getenv("CLIENT_SECRET")

	accessToken, err := getAccessToken(clientID, clientSecret)
	if err != nil {
		log.Printf("Error getting access token: %v\n", err)
		return events.APIGatewayProxyResponse{StatusCode: http.StatusInternalServerError}, err
	}

	boxResponse, err := getData(accessToken)
	if err != nil {
		log.Printf("Error getting data: %v\n", err)
		return events.APIGatewayProxyResponse{StatusCode: http.StatusInternalServerError}, err
	}

	fmt.Printf("%+v\n", boxResponse)

	var fileNames, folderNames, ownerIDs []string
	// Perform according to business requirements
	for _, entry := range boxResponse.Entries {
		if strings.Contains(entry.Name, ".") {
			fileNames = append(fileNames, entry.Name)
		} else {
			folderNames = append(folderNames, entry.Name)
		}
		ownerIDs = append(ownerIDs, entry.OwnedBy.ID)
	}

	fmt.Println(fileNames)
	fmt.Println(folderNames)
	fmt.Println(ownerIDs)
	awsRegion := os.Getenv("AWS_REGION")
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})
	if err != nil {
		log.Printf("Error creating AWS session: %v\n", err)
		return events.APIGatewayProxyResponse{StatusCode: http.StatusInternalServerError}, err
	}

	kinesisClient := kinesis.New(sess)
	err = publishToKinesis(kinesisClient, boxResponse)
	if err != nil {
		log.Printf("Error publishing to Kinesis: %v\n", err)
		return events.APIGatewayProxyResponse{StatusCode: http.StatusInternalServerError}, err
	}

	return events.APIGatewayProxyResponse{StatusCode: http.StatusOK}, nil
}

func main() {
	lambda.Start(handler)
}
