package openapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pb33f/libopenapi"
	"github.com/pb33f/libopenapi/datamodel"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
)

const maximumSpecificationSize = 32 << 20

func loadDocument(specFile string, headers map[string]string, client *http.Client, timeout time.Duration) (*v3.Document, error) {
	if isRemoteSpecification(specFile) {
		return loadRemoteDocument(specFile, headers, client, timeout)
	}
	return loadLocalDocument(specFile)
}

func loadLocalDocument(specFile string) (*v3.Document, error) {
	absolutePath, err := filepath.Abs(specFile)
	if err != nil {
		return nil, fmt.Errorf("resolve OpenAPI specification path: %w", err)
	}
	file, err := os.Open(absolutePath)
	if err != nil {
		return nil, fmt.Errorf("read OpenAPI specification: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("read OpenAPI specification metadata: %w", err)
	}
	if info.Size() > maximumSpecificationSize {
		return nil, fmt.Errorf("read OpenAPI specification: document exceeds %d bytes", maximumSpecificationSize)
	}
	specification, err := readLimitedSpecification(file)
	if err != nil {
		return nil, fmt.Errorf("read OpenAPI specification: %w", err)
	}

	return buildDocument(specification, &datamodel.DocumentConfiguration{
		BasePath:            filepath.Dir(absolutePath),
		SpecFilePath:        absolutePath,
		AllowFileReferences: true,
	})
}

func loadRemoteDocument(specURL string, headers map[string]string, client *http.Client, timeout time.Duration) (*v3.Document, error) {
	parsed, err := url.Parse(specURL)
	if err != nil {
		return nil, fmt.Errorf("parse OpenAPI specification URL: %w", err)
	}
	requestContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build OpenAPI specification request: %w", err)
	}
	request.Header.Set("Accept", "application/json, application/yaml, application/x-yaml, text/yaml")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	downloadClient := sameOriginHTTPClient(client, parsed, "OpenAPI specification")
	response, err := downloadClient.Do(request)
	if err != nil {
		if requestContext.Err() != nil {
			return nil, fmt.Errorf("download OpenAPI specification: %w", requestContext.Err())
		}
		return nil, fmt.Errorf("download OpenAPI specification: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("download OpenAPI specification: HTTP status %d", response.StatusCode)
	}
	if response.ContentLength > maximumSpecificationSize {
		return nil, fmt.Errorf("download OpenAPI specification: document exceeds %d bytes", maximumSpecificationSize)
	}
	specification, err := readLimitedSpecification(response.Body)
	if err != nil {
		return nil, fmt.Errorf("download OpenAPI specification: %w", err)
	}
	return buildDocument(specification, &datamodel.DocumentConfiguration{})
}

func readLimitedSpecification(reader io.Reader) ([]byte, error) {
	specification, err := io.ReadAll(io.LimitReader(reader, maximumSpecificationSize+1))
	if err != nil {
		return nil, err
	}
	if len(specification) > maximumSpecificationSize {
		return nil, fmt.Errorf("document exceeds %d bytes", maximumSpecificationSize)
	}
	return specification, nil
}

func sameOriginHTTPClient(client *http.Client, origin *url.URL, purpose string) *http.Client {
	cloned := *client
	configuredRedirect := client.CheckRedirect
	cloned.CheckRedirect = func(next *http.Request, previous []*http.Request) error {
		if !sameOrigin(origin, next.URL) {
			return fmt.Errorf("%s redirect changed origin", purpose)
		}
		if configuredRedirect != nil {
			return configuredRedirect(next, previous)
		}
		if len(previous) >= 10 {
			return fmt.Errorf("stopped after 10 %s redirects", purpose)
		}
		return nil
	}
	return &cloned
}

func sameOrigin(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}

func buildDocument(specification []byte, configuration *datamodel.DocumentConfiguration) (*v3.Document, error) {
	document, err := libopenapi.NewDocumentWithConfiguration(
		specification,
		configuration,
	)
	if err != nil {
		return nil, fmt.Errorf("parse OpenAPI specification: %w", err)
	}

	model, err := document.BuildV3Model()
	if err != nil {
		return nil, fmt.Errorf("build OpenAPI specification model: %w", err)
	}
	if model == nil {
		return nil, errors.New("build OpenAPI specification model: no document returned")
	}
	return &model.Model, nil
}
