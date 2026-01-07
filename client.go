package uploader

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type UploadClient struct {
	accessKey  string
	chunkSize  int
	endpoint   string
	httpClient *http.Client
	logger     *log.Logger
}

type IUploadClient interface {
	DeleteObject(bucketName, objectName string) error
	DownloadObject(bucketName, objectName string, w io.Writer) error
	GetObject(bucketName, objectName string) ([]byte, error)
	ListObjects(bucketName, path string) ([]ObjectInfo, error)
	PutObject(bucketName, objectName string, data []byte, opts *UploadOptions) (*UploadResult, error)
	StatObject(bucketName, objectName string) (*ObjectInfo, error)
}

// NewClient creates a new upload client
func NewClient(options *UploadClientOptions) *UploadClient {
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{
			Timeout: 30 * time.Second, // 30 seconds timeout
		}
	}

	if options.Logger == nil {
		options.Logger = log.New(log.Writer(), "[UPLOADER] ", log.Flags())
	}

	return &UploadClient{
		accessKey:  options.AccessKey,
		chunkSize:  options.ChunkSize,
		endpoint:   options.Endpoint,
		httpClient: options.HTTPClient,
		logger:     options.Logger,
	}
}

// NewClientWithDefaults creates a new upload client with default options
func NewClientWithDefaults(endpoint, accessKey string) *UploadClient {
	return NewClient(&UploadClientOptions{
		AccessKey: accessKey,
		ChunkSize: 1024 * 1024, // 1MB default
		Endpoint:  endpoint,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second, // 30 seconds timeout
		},
		Logger: log.New(log.Writer(), "[UPLOADER] ", log.Flags()),
	})
}

// CalculateChecksum calculates file checksum using selected algorithm
func (c *UploadClient) calculateChecksum(data []byte, algorithm string) (string, error) {
	switch algorithm {
	case Sha256Sum, "":
		hash := sha256.Sum256(data)
		return hex.EncodeToString(hash[:]), nil
	case Md5Sum:
		hash := md5.Sum(data)
		return hex.EncodeToString(hash[:]), nil
	default:
		c.logger.Printf("Unsupported algorithm: %s", algorithm)
		return "", ErrUnsupportedAlgorithm
	}
}

// DeleteObject calls DeleteFile handler on server
func (c *UploadClient) DeleteObject(bucketName, objectName string) error {
	url := fmt.Sprintf("%s/upload/delete", c.endpoint)

	reqBody := deleteFileRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.Printf("Failed to marshal request body: %v", err)
		return ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send request: %v", err)
		return ErrFailedToConnect
	}
	defer resp.Body.Close()

	return c.parseServerResponse(resp, nil)
}

// DownloadObject streams object to provided writer (e.g. file)
func (c *UploadClient) DownloadObject(bucketName, objectName string, w io.Writer) error {
	url := fmt.Sprintf("%s/upload/download", c.endpoint)

	reqBody := downloadFileRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return ErrFailedToConnect
	}
	defer resp.Body.Close()

	err = c.parseServerResponse(resp, nil)
	if err != nil {
		return err
	}

	if _, err := io.Copy(w, resp.Body); err != nil {
		c.logger.Printf("Failed to stream object: %v", err)
		return ErrClientFailedToReadObject
	}

	return nil
}

// GetObject calls server download handler and returns bytes
func (c *UploadClient) GetObject(bucketName, objectName string) ([]byte, error) {
	url := fmt.Sprintf("%s/upload/download", c.endpoint)

	reqBody := downloadFileRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send request: %v", err)
		return nil, ErrFailedToConnect
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = c.parseServerResponse(resp, nil)
		if err != nil {
			return nil, err
		}

		return nil, ErrUnknown
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		c.logger.Printf("Failed to read object body: %v", err)
		return nil, ErrClientFailedToReadObject
	}

	return data, nil
}

// ListObjects lists objects in a bucket
func (c *UploadClient) ListObjects(bucketName, path string) ([]ObjectInfo, error) {
	var result []ObjectInfo

	url := fmt.Sprintf("%s/upload/list", c.endpoint)

	reqBody := listObjectsRequest{
		Bucket: bucketName,
		Path:   path,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.Printf("Failed to marshal list objects request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to marshal list objects request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send list objects request: %v", err)
		return nil, ErrFailedToConnect
	}
	defer resp.Body.Close()

	err = c.parseServerResponse(resp, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// parseServerError parses the error response from the server
func (c *UploadClient) parseServerError(err *serverErrorBody) error {
	if err == nil {
		return nil
	}

	switch err.ErrorCode {
	case "ErrNotFound":
		return ErrResourceNotFound
	case "ErrAuthApiKey":
		return ErrUnauthorized
	case "ErrBucketNotFound":
		return ErrBucketNotFound
	case "ErrObjectNotFound":
		return ErrObjectNotFound
	case "ErrObjectFailedToCreateDir":
		return ErrObjectFailedToCreateDir
	case "ErrObjectFailedToCreateObj":
		return ErrObjectFailedToCreateObject
	case "ErrObjectFailedToOpen":
		return ErrObjectFailedToOpen
	case "ErrObjectInvalidDataURI":
		return ErrObjectInvalidDataURI
	}

	return ErrUnknown
}

// parseServerResponse parses the response from the server
func (c *UploadClient) parseServerResponse(resp *http.Response, out any) error {
	var response serverResponse

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.logger.Printf("Failed to read response body: %v", err)
		return ErrParseResponseFailed
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		c.logger.Printf("Failed to unmarshal response body: %v", err)
		return ErrParseResponseFailed
	}

	if out != nil {
		err = json.Unmarshal(response.Data, out)
		if err != nil {
			c.logger.Printf("Failed to unmarshal response body: %v", err)
			return ErrParseResponseFailed
		}
	}

	return c.parseServerError(response.Error)
}

// PutObject uploads a file in chunks with checksum verification
func (c *UploadClient) PutObject(bucketName, objectName string, data []byte, opts *UploadOptions) (*UploadResult, error) {
	totalSize := int64(len(data))
	reader := bytes.NewReader(data)

	checksumAlgo := Sha256Sum
	if opts != nil && opts.ChecksumAlgorithm != "" {
		checksumAlgo = opts.ChecksumAlgorithm
	}

	checksum, err := c.calculateChecksum(data, checksumAlgo)
	if err != nil {
		c.logger.Printf("Failed to calculate checksum: %v", err)
		return nil, err
	}

	fileId := uuid.New().String()

	var uploaded int64
	chunkNum := 0

	for {
		chunk := make([]byte, c.chunkSize)
		n, err := reader.Read(chunk)
		if err != nil && err != io.EOF {
			c.logger.Printf("Failed to read chunk: %v", err)
			return nil, ErrClientUploadFailed
		}

		if n == 0 {
			break
		}

		if err := c.sendChunk(bucketName, objectName, fileId, chunk[:n], chunkNum, checksum); err != nil {
			c.logger.Printf("Failed to send chunk: %v", err)
			return nil, err
		}

		uploaded += int64(n)
		chunkNum++

		if opts != nil && opts.OnProgress != nil {
			opts.OnProgress(uploaded, totalSize)
		}

		if err == io.EOF {
			break
		}
	}

	if err := c.verifyChecksum(bucketName, objectName, fileId, checksum); err != nil {
		return nil, ErrChecksumMismatch
	}

	return &UploadResult{
		Checksum: checksum,
		Size:     totalSize,
	}, nil
}

// SendChunk sends single chunk to server
func (c *UploadClient) sendChunk(bucketName, objectName, fileId string, chunk []byte, chunkNum int, checksum string) error {
	url := fmt.Sprintf("%s/upload/chunk", c.endpoint)

	encodedChunk := base64.StdEncoding.EncodeToString(chunk)

	payload := chunkRequest{
		BucketName: bucketName,
		ObjectName: objectName,
		FileId:     fileId,
		ChunkNum:   chunkNum,
		Data:       encodedChunk,
		Checksum:   checksum,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		c.logger.Printf("Failed to marshal chunk request: %v", err)
		return ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to connect to server: %v", err)
		return ErrFailedToConnect
	}
	defer resp.Body.Close()

	return c.parseServerResponse(resp, nil)
}

// StatObject returns information about an object in a bucket
func (c *UploadClient) StatObject(bucketName, objectName string) (*ObjectInfo, error) {
	var result ObjectInfo

	url := fmt.Sprintf("%s/upload/stat", c.endpoint)

	payload := statObjectRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		c.logger.Printf("Failed to marshal stat request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send request: %v", err)
		return nil, ErrFailedToConnect
	}
	defer resp.Body.Close()

	err = c.parseServerResponse(resp, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// VerifyChecksum calls server verify endpoint
func (c *UploadClient) verifyChecksum(bucketName, objectName, fileId, expectedChecksum string) error {
	url := fmt.Sprintf("%s/upload/verify", c.endpoint)

	payload := verifyRequest{
		Bucket:   bucketName,
		Path:     objectName,
		FileId:   fileId,
		Checksum: expectedChecksum,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		c.logger.Printf("Failed to marshal verify request: %v", err)
		return ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send request: %v", err)
		return ErrFailedToConnect
	}
	defer resp.Body.Close()

	return c.parseServerResponse(resp, nil)
}
