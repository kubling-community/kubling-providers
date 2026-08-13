package openapi

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateConfigTemplateListsCandidatesAndPreservesSeedPlaceholders(t *testing.T) {
	directory := t.TempDir()
	specPath := filepath.Join(directory, "api.yaml")
	configPath := filepath.Join(directory, "provider.seed.yaml")
	writeTestFile(t, specPath, configTemplateOpenAPISpec)
	writeTestFile(t, configPath, `
specFile: api.yaml
baseUrl: https://api.example.test
namespace: ${TEST_NAMESPACE}
headers:
  Authorization: Bearer ${TEST_API_TOKEN}
`)
	t.Setenv("TEST_NAMESPACE", "expanded-namespace")
	t.Setenv("TEST_API_TOKEN", "expanded-secret")

	generated, err := GenerateConfigTemplate(configPath)
	if err != nil {
		t.Fatalf("GenerateConfigTemplate() error = %v", err)
	}
	content := string(generated)
	for _, expected := range []string{
		"namespace: ${TEST_NAMESPACE}",
		"Authorization: Bearer ${TEST_API_TOKEN}",
		"name: PROJECT",
		"name: ISSUE",
		"name: WARNING",
		"listOperation: getUsage",
		"value: ${OPENAPI_GET_USAGE_START_TIME}",
		"mode: CURSOR",
		"pageSizeParameter: limit",
		"cursorParameter: after",
		"nextCursorPath: /last_id",
		"hasMorePath: /has_more",
		"Skipped: getProject: path parameters are not supported by list entities",
		"Skipped: health: successful JSON response contains no object arrays",
	} {
		if !strings.Contains(content, expected) {
			t.Errorf("generated config does not contain %q:\n%s", expected, content)
		}
	}
	if strings.Contains(content, "expanded-secret") || strings.Contains(content, "expanded-namespace") {
		t.Fatalf("generated config expanded seed placeholders:\n%s", content)
	}

	generatedPath := filepath.Join(directory, "provider.generated.yaml")
	writeTestFile(t, generatedPath, content)
	t.Setenv("OPENAPI_GET_USAGE_START_TIME", "1730419200")
	config, err := LoadConfig(generatedPath)
	if err != nil {
		t.Fatalf("LoadConfig(generated) error = %v", err)
	}
	provider, err := New(config)
	if err != nil {
		t.Fatalf("New(generated) error = %v", err)
	}
	if len(provider.catalog.entities) != 4 {
		t.Fatalf("generated entity count = %d, want 4", len(provider.catalog.entities))
	}
}

func TestGenerateConfigTemplateRejectsDocumentWithoutCandidates(t *testing.T) {
	directory := t.TempDir()
	writeTestFile(t, filepath.Join(directory, "api.yaml"), `
openapi: 3.1.0
info:
  title: Empty API
  version: 1.0.0
paths:
  /health:
    get:
      operationId: health
      responses:
        "204":
          description: Healthy.
`)
	configPath := filepath.Join(directory, "provider.yaml")
	writeTestFile(t, configPath, `
specFile: api.yaml
baseUrl: https://api.example.test
discovery:
  enabled: true
`)

	_, err := GenerateConfigTemplate(configPath)
	if err == nil || !strings.Contains(err.Error(), "contains no configurable GET entity candidates") {
		t.Fatalf("GenerateConfigTemplate() error = %v, want no candidates error", err)
	}
}

const configTemplateOpenAPISpec = `
openapi: 3.1.0
info:
  title: Template API
  version: 1.0.0
paths:
  /projects:
    get:
      operationId: listProjects
      parameters:
        - name: limit
          in: query
          schema:
            type: integer
        - name: after
          in: query
          schema:
            type: string
      responses:
        "200":
          description: Project page.
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: array
                    items:
                      $ref: "#/components/schemas/Project"
                  last_id:
                    type: string
                  has_more:
                    type: boolean
  /search:
    get:
      operationId: search
      responses:
        "200":
          description: Search results.
          content:
            application/json:
              schema:
                type: object
                properties:
                  issues:
                    type: array
                    items:
                      $ref: "#/components/schemas/Issue"
                  warnings:
                    type: array
                    items:
                      $ref: "#/components/schemas/Warning"
  /usage:
    get:
      operationId: getUsage
      parameters:
        - name: start_time
          in: query
          required: true
          schema:
            type: integer
      responses:
        "200":
          description: Usage buckets.
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: array
                    items:
                      type: object
                      title: UsageBucket
                      properties:
                        start_time:
                          type: integer
  /projects/{project_id}:
    get:
      operationId: getProject
      parameters:
        - name: project_id
          in: path
          required: true
          schema:
            type: string
      responses:
        "200":
          description: One project.
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Project"
  /health:
    get:
      operationId: health
      responses:
        "200":
          description: Health status.
          content:
            application/json:
              schema:
                type: object
                properties:
                  status:
                    type: string
components:
  schemas:
    Project:
      type: object
      properties:
        id:
          type: string
    Issue:
      type: object
      properties:
        id:
          type: string
    Warning:
      type: object
      properties:
        code:
          type: string
`
