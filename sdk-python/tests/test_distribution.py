"""Run against an installed wheel or sdist, outside the generated source tree."""

import importlib
from importlib import metadata, resources
import unittest

import grpc
from kubling.provider.v1 import (
    capabilities_pb2,
    expression_pb2,
    provider_pb2_grpc,
    tuple_pb2,
)
from kubling.v1 import value_pb2


class DistributionTest(unittest.TestCase):
    def test_distribution_contains_all_provider_modules(self):
        self.assertTrue(metadata.version("kubling-provider-grpc"))
        for name in (
            "capabilities",
            "connection",
            "expression",
            "lob",
            "metadata",
            "mutation",
            "provider",
            "query",
            "semantic",
            "tuple",
        ):
            importlib.import_module(f"kubling.provider.v1.{name}_pb2")
        package = resources.files("kubling.provider")
        self.assertTrue(
            package.joinpath("proto/kubling/provider/v1/provider.proto").is_file()
        )

    def test_uses_shared_typed_values(self):
        integer_type = value_pb2.TypeDescriptor(type=value_pb2.VALUE_TYPE_INTEGER)
        array_type = value_pb2.TypeDescriptor(
            type=value_pb2.VALUE_TYPE_ARRAY,
            element_type=integer_type,
        )
        value = value_pb2.Value(
            array_value=value_pb2.ArrayValue(
                element_type=integer_type,
                elements=[
                    value_pb2.Value(integer_value=7),
                    value_pb2.Value(null_value=value_pb2.NullValue()),
                ],
            )
        )
        batch = tuple_pb2.TupleBatch(
            fields=[
                tuple_pb2.Field(
                    name="numbers",
                    type=value_pb2.VALUE_TYPE_ARRAY,
                    type_descriptor=array_type,
                )
            ],
            tuples=[tuple_pb2.Tuple(values=[value])],
        )
        decoded = tuple_pb2.TupleBatch.FromString(batch.SerializeToString())
        self.assertEqual(decoded.fields[0].type_descriptor, array_type)
        self.assertEqual(decoded.tuples[0].values[0].WhichOneof("kind"), "array_value")

        literal = expression_pb2.Literal(
            value=value_pb2.Value(null_value=value_pb2.NullValue()),
            declared_type=value_pb2.TypeDescriptor(type=value_pb2.VALUE_TYPE_CLOB),
        )
        self.assertTrue(
            expression_pb2.Literal.FromString(literal.SerializeToString()).HasField(
                "declared_type"
            )
        )
        capabilities = capabilities_pb2.ValueCapabilities(
            features=["array_values_v1"], max_array_dimensions=2
        )
        self.assertTrue(
            capabilities_pb2.ValueCapabilities.FromString(
                capabilities.SerializeToString()
            ).HasField("max_array_dimensions")
        )

    def test_constructs_provider_stub_without_connecting(self):
        with grpc.insecure_channel("localhost:1") as channel:
            provider = provider_pb2_grpc.ProviderServiceStub(channel)
            self.assertIsInstance(provider.Query, grpc.UnaryStreamMultiCallable)
            self.assertIsInstance(provider.ReadLob, grpc.UnaryStreamMultiCallable)
            self.assertIsInstance(provider.ReleaseLob, grpc.UnaryUnaryMultiCallable)


if __name__ == "__main__":
    unittest.main()
