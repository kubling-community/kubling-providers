package openapi

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

func operationURL(baseURL, operationPath string) (string, error) {
	return operationURLWithParameters(baseURL, operationPath, nil)
}

func operationURLWithParameters(baseURL, operationPath string, parameters map[string]string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse datasource base URL: %w", err)
	}
	if (!strings.EqualFold(base.Scheme, "http") && !strings.EqualFold(base.Scheme, "https")) ||
		base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return "", fmt.Errorf("datasource base URL must be an absolute HTTP or HTTPS URL without user information, query or fragment")
	}
	reference, err := url.Parse(operationPath)
	if err != nil {
		return "", fmt.Errorf("parse operation path %q: %w", operationPath, err)
	}
	if reference.IsAbs() || reference.Host != "" || reference.Opaque != "" {
		return "", fmt.Errorf("operation path %q changed datasource origin", operationPath)
	}
	if reference.RawQuery != "" || reference.ForceQuery || reference.Fragment != "" || strings.ContainsAny(operationPath, "?#") {
		return "", fmt.Errorf("operation path %q must not contain a query or fragment", operationPath)
	}

	expandedPath, err := expandOperationPath(operationPath, parameters)
	if err != nil {
		return "", err
	}
	basePath := strings.TrimRight(base.EscapedPath(), "/")
	combinedPath := basePath + "/" + strings.TrimLeft(expandedPath, "/")
	decodedPath, err := url.PathUnescape(combinedPath)
	if err != nil {
		return "", fmt.Errorf("decode expanded operation path: %w", err)
	}

	resolved := *base
	resolved.Path = decodedPath
	resolved.RawPath = combinedPath
	resolved.RawQuery = ""
	resolved.ForceQuery = false
	resolved.Fragment = ""
	return resolved.String(), nil
}

func expandOperationPath(template string, parameters map[string]string) (string, error) {
	leadingSlash := strings.HasPrefix(template, "/")
	rawPath := strings.TrimPrefix(template, "/")
	rawSegments := strings.Split(rawPath, "/")
	encodedSegments := make([]string, len(rawSegments))
	used := make(map[string]struct{}, len(parameters))
	for index, segment := range rawSegments {
		encoded, err := expandOperationPathSegment(segment, parameters, used)
		if err != nil {
			return "", fmt.Errorf("operation path segment %d: %w", index, err)
		}
		encodedSegments[index] = encoded
	}
	unused := make([]string, 0)
	for name := range parameters {
		if _, exists := used[name]; !exists {
			unused = append(unused, name)
		}
	}
	if len(unused) > 0 {
		sort.Strings(unused)
		return "", fmt.Errorf("path parameter %q is not present in operation path %q", unused[0], template)
	}
	expanded := strings.Join(encodedSegments, "/")
	if leadingSlash {
		expanded = "/" + expanded
	}
	return expanded, nil
}

func expandOperationPathSegment(
	segment string,
	parameters map[string]string,
	used map[string]struct{},
) (string, error) {
	var decoded strings.Builder
	for position := 0; position < len(segment); {
		open := strings.IndexByte(segment[position:], '{')
		if open < 0 {
			if strings.Contains(segment[position:], "}") {
				return "", fmt.Errorf("contains an unmatched }")
			}
			literal, err := url.PathUnescape(segment[position:])
			if err != nil {
				return "", fmt.Errorf("decode literal path text: %w", err)
			}
			decoded.WriteString(literal)
			break
		}
		open += position
		if strings.Contains(segment[position:open], "}") {
			return "", fmt.Errorf("contains an unmatched }")
		}
		literal, err := url.PathUnescape(segment[position:open])
		if err != nil {
			return "", fmt.Errorf("decode literal path text: %w", err)
		}
		decoded.WriteString(literal)
		closeOffset := strings.IndexByte(segment[open+1:], '}')
		if closeOffset < 0 {
			return "", fmt.Errorf("contains an unclosed path parameter")
		}
		closePosition := open + 1 + closeOffset
		name := segment[open+1 : closePosition]
		if name == "" || strings.ContainsAny(name, "{}/") {
			return "", fmt.Errorf("contains invalid path parameter %q", name)
		}
		value, exists := parameters[name]
		if !exists {
			return "", fmt.Errorf("path parameter %q is unbound", name)
		}
		if value == "" {
			return "", fmt.Errorf("path parameter %q is empty", name)
		}
		decoded.WriteString(value)
		used[name] = struct{}{}
		position = closePosition + 1
	}

	value := decoded.String()
	if value == "." || value == ".." {
		return "", fmt.Errorf("path segment %q is not allowed", value)
	}
	return url.PathEscape(value), nil
}
