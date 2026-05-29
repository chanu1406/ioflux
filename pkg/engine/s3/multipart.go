package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/chanuollala/ioflux/pkg/engine"
)

func (e *S3Engine) putMultipart(ctx context.Context, key string, r io.Reader, size int64) error {
	createOut, err := e.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(e.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return mapErr("create multipart "+key, err)
	}
	if createOut.UploadId == nil || *createOut.UploadId == "" {
		return fmt.Errorf("s3: create multipart %q: empty upload id", key)
	}
	uploadID := *createOut.UploadId

	abort := func() {
		_, _ = e.client.AbortMultipartUpload(context.Background(), &awss3.AbortMultipartUploadInput{
			Bucket:   aws.String(e.cfg.Bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
	}

	partBuf := make([]byte, e.cfg.MultipartPartSize)
	remaining := size
	partNumber := int32(1)
	parts := make([]types.CompletedPart, 0, int((size+e.cfg.MultipartPartSize-1)/e.cfg.MultipartPartSize))

	for remaining > 0 {
		want := e.cfg.MultipartPartSize
		if remaining < want {
			want = remaining
		}
		n, readErr := io.ReadFull(r, partBuf[:want])
		if readErr != nil {
			abort()
			if errors.Is(readErr, io.ErrUnexpectedEOF) || errors.Is(readErr, io.EOF) {
				return fmt.Errorf("s3: multipart put %q: source ended after %d fewer bytes than declared: %w", key, remaining-int64(n), engine.ErrShortRead)
			}
			return fmt.Errorf("s3: multipart put %q: read source: %w", key, readErr)
		}

		uploadOut, err := e.client.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket:        aws.String(e.cfg.Bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(partNumber),
			Body:          bytes.NewReader(partBuf[:n]),
			ContentLength: aws.Int64(int64(n)),
		})
		if err != nil {
			abort()
			return mapErr(fmt.Sprintf("upload part %d for %s", partNumber, key), err)
		}
		if uploadOut.ETag == nil || *uploadOut.ETag == "" {
			abort()
			return fmt.Errorf("s3: upload part %d for %q: missing ETag", partNumber, key)
		}
		parts = append(parts, types.CompletedPart{
			ETag:       uploadOut.ETag,
			PartNumber: aws.Int32(partNumber),
		})

		remaining -= int64(n)
		partNumber++
	}

	_, err = e.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String(e.cfg.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		abort()
		return mapErr("complete multipart "+key, err)
	}
	return nil
}
