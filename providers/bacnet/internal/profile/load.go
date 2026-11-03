package profile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	builtinprofiles "github.com/kubling-community/kubling-providers/providers/bacnet/profiles"
	"gopkg.in/yaml.v3"
)

// Load compiles the built-in standard profile together with external profile
// documents. Every external path contains exactly one strict YAML document.
func Load(paths []string) (*Catalog, error) {
	standard, err := decodeDocument(bytes.NewReader(builtinprofiles.Standard()), "built-in standard profile")
	if err != nil {
		return nil, err
	}
	documents := make([]document, 0, len(paths)+1)
	documents = append(documents, standard)
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open BACnet profile %q: %w", path, err)
		}
		document, decodeErr := decodeDocument(file, path)
		closeErr := file.Close()
		if decodeErr != nil {
			return nil, decodeErr
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close BACnet profile %q: %w", path, closeErr)
		}
		documents = append(documents, document)
	}
	return compile(documents)
}

func decodeDocument(reader io.Reader, source string) (document, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	var result document
	if err := decoder.Decode(&result); err != nil {
		return document{}, fmt.Errorf("decode BACnet profile %q: %w", source, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return document{}, fmt.Errorf(
				"decode BACnet profile %q: multiple YAML documents are not supported",
				source,
			)
		}
		return document{}, fmt.Errorf("decode BACnet profile %q: %w", source, err)
	}
	result.source = source
	return result, nil
}
