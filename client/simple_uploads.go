package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
)

// UploadBytesToPresignedURLWithHeaders uploads bytes to a presigned URL with optional custom headers.
func (c *Client) UploadBytesToPresignedURLWithHeaders(ctx context.Context, presignedURL string, data []byte, headers map[string]string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")

	// Add custom headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := c.DoS3Request(ctx, req)
	if err != nil {
		return fmt.Errorf("uploading: %w", err)
	}

	defer deferCloseBody(resp)

	return checkResponse(resp, http.StatusOK, http.StatusNoContent)
}

// UploadListingToPresignedURL compresses a NAR listing with zstd and uploads it with Content-Encoding header.
// The listing is stored as a .ls file, compatible with Nix's lazy NAR accessor format.
func (c *Client) UploadListingToPresignedURL(ctx context.Context, presignedURL string, listing *NarListing) error {
	// Compress listing with zstd
	compressed, err := CompressListingWithZstd(listing)
	if err != nil {
		return fmt.Errorf("compressing listing: %w", err)
	}

	// Upload with Content-Encoding header
	headers := map[string]string{
		"Content-Encoding": compressionZstd,
	}

	return c.UploadBytesToPresignedURLWithHeaders(ctx, presignedURL, compressed, headers)
}

// UploadBuildLogToPresignedURL uploads a compressed build log with Content-Encoding header.
// This follows Nix's convention for compressed build logs stored at log/<drvPath>.
// The compressedInfo must point to a temporary file created by CompressBuildLog.
func (c *Client) UploadBuildLogToPresignedURL(ctx context.Context, presignedURL string, compressedInfo *CompressedBuildLogInfo) error {
	stat, err := os.Stat(compressedInfo.TempFile)
	if err != nil {
		return fmt.Errorf("stat compressed log: %w", err)
	}

	fileSize := stat.Size()

	file, err := os.Open(compressedInfo.TempFile)
	if err != nil {
		return fmt.Errorf("opening compressed log: %w", err)
	}

	// The transport may still read the body after Do returns. A read from a
	// closed file is an error, a read from an unmapped page is a crash.
	defer func() { _ = file.Close() }()

	// Not the *os.File itself: net/http would use sendfile(2), which darwin's
	// Nix sandbox refuses. ReadAt gives each attempt its own offset.
	newBody := func() io.ReadCloser {
		if fileSize == 0 {
			return http.NoBody
		}

		return io.NopCloser(io.NewSectionReader(file, 0, fileSize))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, newBody())
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.ContentLength = fileSize
	req.GetBody = func() (io.ReadCloser, error) { return newBody(), nil }

	// Set headers
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Content-Encoding", compressionZstd)

	// Upload
	resp, err := c.DoS3Request(ctx, req)
	if err != nil {
		return fmt.Errorf("uploading: %w", err)
	}
	defer deferCloseBody(resp)

	return checkResponse(resp, http.StatusOK, http.StatusCreated, http.StatusNoContent)
}
