package uploader

import "errors"

var (
	// Server errors
	ErrBucketNotFound             = errors.New("bucket not found")
	ErrChecksumMismatch           = errors.New("checksum verification failed")
	ErrObjectNotFound             = errors.New("object not found")
	ErrObjectFailedToCreateDir    = errors.New("failed to create directory")
	ErrObjectFailedToCreateObject = errors.New("failed to create object")
	ErrObjectFailedToOpen         = errors.New("failed to open object")
	ErrObjectInvalidDataURI       = errors.New("invalid data URI")
	ErrParseResponseFailed        = errors.New("failed to parse server response")
	ErrResourceNotFound           = errors.New("resource not found")
	ErrUnauthorized               = errors.New("unauthorized")
	ErrUnknown                    = errors.New("unknown server error")

	// Client errors
	ErrFailedToConnect          = errors.New("failed to connect to server")
	ErrFailedToParseRequest     = errors.New("failed to parse request")
	ErrClientFailedToReadObject = errors.New("failed to read object")
	ErrUnsupportedAlgorithm     = errors.New("unsupported algorithm")
	ErrClientUploadFailed       = errors.New("upload failed")
)
