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
	"net/http"

	"github.com/google/uuid"
)

type UploadClient struct {
	accessKey  string
	chunkSize  int
	endpoint   string
	httpClient *http.Client
}

type IUploadClient interface {
	DeleteObject(bucketName, objectName string) error
	DownloadObject(bucketName, objectName string, w io.Writer) error
	GetObject(bucketName, objectName string) ([]byte, error)
	ListObjects(bucketName, path string) ([]ObjectInfo, error)
	StatObject(bucketName, objectName string) (*ObjectInfo, error)
	PutObject(bucketName, objectName string, data []byte, opts *UploadOptions) (*UploadResult, error)
	SendChunk(bucketName, objectName, fileId string, chunk []byte, chunkNum int, checksum string) error
}

// NewClient creates a new upload client
func NewClient(options *UploadClientOptions) *UploadClient {
	return &UploadClient{
		accessKey:  options.AccessKey,
		chunkSize:  options.ChunkSize,
		endpoint:   options.Endpoint,
		httpClient: options.HTTPClient,
	}
}

// NewClientWithDefaults creates a new upload client with default options
func NewClientWithDefaults(endpoint, accessKey string) *UploadClient {
	return NewClient(&UploadClientOptions{
		AccessKey:  accessKey,
		ChunkSize:  1024 * 1024, // 1MB default
		Endpoint:   endpoint,
		HTTPClient: &http.Client{},
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
		return "", fmt.Errorf("unsupported algorithm: %s", algorithm)
	}
}

// DeleteObject calls DeleteFile handler on server
func (c *UploadClient) DeleteObject(bucketName, objectName string) error {
	url := fmt.Sprintf("%s/upload/file", c.endpoint)

	reqBody := DeleteFileRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal delete request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DownloadObject streams object to provided writer (e.g. file)
func (c *UploadClient) DownloadObject(bucketName, objectName string, w io.Writer) error {
	url := fmt.Sprintf("%s/upload/download", c.endpoint)

	reqBody := DownloadFileRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal download request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("download failed with status %d: %s", resp.StatusCode, string(body))
	}

	if _, err := io.Copy(w, resp.Body); err != nil {
		return fmt.Errorf("failed to stream object: %w", err)
	}

	return nil
}

// GetObject calls server download handler and returns bytes
func (c *UploadClient) GetObject(bucketName, objectName string) ([]byte, error) {
	url := fmt.Sprintf("%s/upload/download", c.endpoint)

	reqBody := DownloadFileRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal get object request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get object failed with status %d: %s", resp.StatusCode, string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	return data, nil
}

// ListObjects lists objects in a bucket
func (c *UploadClient) ListObjects(bucketName, path string) ([]ObjectInfo, error) {
	url := fmt.Sprintf("%s/upload/list", c.endpoint)

	reqBody := ListObjectsRequest{
		Bucket: bucketName,
		Path:   path,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal get object request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list objects failed with status %d: %s", resp.StatusCode, string(body))
	}

	var objects []ObjectInfo
	err = json.NewDecoder(resp.Body).Decode(&objects)
	if err != nil {
		return nil, fmt.Errorf("failed to decode list objects response: %w", err)
	}

	return objects, nil
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
		return nil, fmt.Errorf("failed to calculate checksum: %w", err)
	}

	fileId := uuid.New().String()

	var uploaded int64
	chunkNum := 0

	for {
		chunk := make([]byte, c.chunkSize)
		n, err := reader.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read chunk: %w", err)
		}

		if n == 0 {
			break
		}

		if err := c.SendChunk(bucketName, objectName, fileId, chunk[:n], chunkNum, checksum); err != nil {
			return nil, fmt.Errorf("failed to send chunk %d: %w", chunkNum, err)
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
		return nil, fmt.Errorf("checksum verification failed: %w", err)
	}

	return &UploadResult{
		Checksum: checksum,
		Size:     totalSize,
	}, nil
}

// SendChunk sends single chunk to server
func (c *UploadClient) SendChunk(bucketName, objectName, fileId string, chunk []byte, chunkNum int, checksum string) error {
	url := fmt.Sprintf("%s/upload/chunk", c.endpoint)

	encodedChunk := base64.StdEncoding.EncodeToString(chunk)

	payload := ChunkRequest{
		BucketName: bucketName,
		ObjectName: objectName,
		FileId:     fileId,
		ChunkNum:   chunkNum,
		Data:       encodedChunk,
		Checksum:   checksum,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal chunk request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// StatObject returns information about an object in a bucket
func (c *UploadClient) StatObject(bucketName, objectName string) (*ObjectInfo, error) {
	url := fmt.Sprintf("%s/upload/stat", c.endpoint)

	payload := StatObjectRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal stat request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var info ObjectInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode stat response: %w", err)
	}

	return &info, nil
}

// VerifyChecksum calls server verify endpoint
func (c *UploadClient) verifyChecksum(bucketName, objectName, fileId, expectedChecksum string) error {
	url := fmt.Sprintf("%s/upload/verify", c.endpoint)

	payload := VerifyRequest{
		Bucket:   bucketName,
		Path:     objectName,
		FileId:   fileId,
		Checksum: expectedChecksum,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal verify request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("verification failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
