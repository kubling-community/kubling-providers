package kubernetes

import (
	"context"
	"errors"
	"fmt"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func inferValueType(value *kublingv1.Value) (kublingv1.ValueType, error) {
	if value == nil {
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN, fmt.Errorf("literal value is required")
	}
	switch value.GetKind().(type) {
	case *kublingv1.Value_StringValue:
		return kublingv1.ValueType_VALUE_TYPE_STRING, nil
	case *kublingv1.Value_VarbinaryValue:
		return kublingv1.ValueType_VALUE_TYPE_VARBINARY, nil
	case *kublingv1.Value_CharValue:
		return kublingv1.ValueType_VALUE_TYPE_CHAR, nil
	case *kublingv1.Value_BooleanValue:
		return kublingv1.ValueType_VALUE_TYPE_BOOLEAN, nil
	case *kublingv1.Value_ByteValue:
		return kublingv1.ValueType_VALUE_TYPE_BYTE, nil
	case *kublingv1.Value_ShortValue:
		return kublingv1.ValueType_VALUE_TYPE_SHORT, nil
	case *kublingv1.Value_IntegerValue:
		return kublingv1.ValueType_VALUE_TYPE_INTEGER, nil
	case *kublingv1.Value_LongValue:
		return kublingv1.ValueType_VALUE_TYPE_LONG, nil
	case *kublingv1.Value_BigintegerValue:
		return kublingv1.ValueType_VALUE_TYPE_BIGINTEGER, nil
	case *kublingv1.Value_FloatValue:
		return kublingv1.ValueType_VALUE_TYPE_FLOAT, nil
	case *kublingv1.Value_DoubleValue:
		return kublingv1.ValueType_VALUE_TYPE_DOUBLE, nil
	case *kublingv1.Value_BigdecimalValue:
		return kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL, nil
	case *kublingv1.Value_DateValue:
		return kublingv1.ValueType_VALUE_TYPE_DATE, nil
	case *kublingv1.Value_TimeValue:
		return kublingv1.ValueType_VALUE_TYPE_TIME, nil
	case *kublingv1.Value_TimestampValue:
		return kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, nil
	case *kublingv1.Value_BlobValue:
		return kublingv1.ValueType_VALUE_TYPE_BLOB, nil
	case *kublingv1.Value_ClobValue:
		return kublingv1.ValueType_VALUE_TYPE_CLOB, nil
	case *kublingv1.Value_XmlValue:
		return kublingv1.ValueType_VALUE_TYPE_XML, nil
	case *kublingv1.Value_GeometryValue:
		return kublingv1.ValueType_VALUE_TYPE_GEOMETRY, nil
	case *kublingv1.Value_GeographyValue:
		return kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY, nil
	case *kublingv1.Value_JsonValue:
		return kublingv1.ValueType_VALUE_TYPE_JSON, nil
	default:
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN, fmt.Errorf("cannot infer literal type")
	}
}

func kubernetesOperationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	code := codes.Unavailable
	switch {
	case apierrors.IsNotFound(err):
		code = codes.NotFound
	case apierrors.IsAlreadyExists(err):
		code = codes.AlreadyExists
	case apierrors.IsConflict(err):
		code = codes.Aborted
	case apierrors.IsUnauthorized(err):
		code = codes.Unauthenticated
	case apierrors.IsForbidden(err):
		code = codes.PermissionDenied
	case apierrors.IsBadRequest(err), apierrors.IsInvalid(err):
		code = codes.InvalidArgument
	case apierrors.IsTooManyRequests(err):
		code = codes.ResourceExhausted
	case apierrors.IsMethodNotSupported(err):
		code = codes.Unimplemented
	}
	return status.Errorf(code, "%s: %v", operation, err)
}
