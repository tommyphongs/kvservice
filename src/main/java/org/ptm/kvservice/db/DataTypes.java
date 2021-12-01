package org.ptm.kvservice.db;

import org.ptm.kvservice.proto.Utils;
import org.ptm.kvservice.utils.FunctionWithException;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.*;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.ByteString;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

@SuppressWarnings("ALL")
public class DataTypes {

    private static final Map<String, Map.Entry<FunctionWithException<byte[], JsonPrimitive>,
            FunctionWithException<JsonPrimitive, byte[]>>> converters;
    private static final Map<String, Map.Entry<FunctionWithException<ByteString, List<JsonPrimitive>>,
            FunctionWithException<List<JsonPrimitive>, ByteString>>> listConverters;

    public static final String INTEGER_TYPE_STR = "integer";
    public static final String LONG_TYPE_STR = "long";
    public static final String DOUBLE_TYPE_STR = "double";
    public static final String STRING_TYPE_STR = "string";
    public static final String BOOLEAN_TYPE_STR = "boolean";

    static {
        Map<String, Map.Entry<FunctionWithException<byte[], JsonPrimitive>, FunctionWithException<JsonPrimitive, byte[]>>>
                convertersMap = new HashMap<>();
        Map<String, Map.Entry<FunctionWithException<ByteString, List<JsonPrimitive>>, FunctionWithException<List<JsonPrimitive>, ByteString>>>
                listConvertorsMap = new HashMap<>();

        convertersMap.put(INTEGER_TYPE_STR, new AbstractMap.SimpleImmutableEntry(new FunctionWithException<byte[], JsonPrimitive>() {
            @Override
            public JsonPrimitive apply(byte[] bytes) {
                return new JsonPrimitive(Ints.fromByteArray(bytes));
            }
        }, new Function<JsonPrimitive, byte[]>() {
            @Override
            public byte[] apply(JsonPrimitive jsonPrimitive) {
                return Ints.toByteArray(jsonPrimitive.getAsInt());
            }
        }));
        listConvertorsMap.put(INTEGER_TYPE_STR, new AbstractMap.SimpleImmutableEntry(
                new FunctionWithException<ByteString, List<JsonPrimitive>>() {
                    @Override
                    public List<JsonPrimitive> apply(ByteString bytes) throws Exception {
                        List<JsonPrimitive> jsonArray = new ArrayList<>();
                        Utils.Ints.parseFrom(bytes).getArrayList().forEach(integer -> {
                            jsonArray.add(new JsonPrimitive(integer));
                        });
                        return jsonArray;
                    }
                },
                new FunctionWithException<List<JsonPrimitive>, ByteString>() {
                    @Override
                    public ByteString apply(List<JsonPrimitive> jsonElements) throws Exception {
                        try {
                            Utils.Ints.Builder builder = Utils.Ints.newBuilder();
                            jsonElements.forEach(jsonElement -> builder.addArray(jsonElement.getAsInt()));
                            return builder.build().toByteString();
                        } catch (NumberFormatException e) {
                            throw new NumberFormatException(String.format("Error when convert to integer value"));
                        }
                    }
                }
        ));

        convertersMap.put(LONG_TYPE_STR, new AbstractMap.SimpleImmutableEntry(new Function<byte[], JsonPrimitive>() {
            @Override
            public JsonPrimitive apply(byte[] bytes) {
                return new JsonPrimitive(Longs.fromByteArray(bytes));
            }
        }, new Function<JsonPrimitive, byte[]>() {
            @Override
            public byte[] apply(JsonPrimitive jsonPrimitive) {
                return Longs.toByteArray(jsonPrimitive.getAsLong());
            }
        }));
        listConvertorsMap.put(LONG_TYPE_STR, new AbstractMap.SimpleImmutableEntry(
                new FunctionWithException<ByteString, List<JsonPrimitive>>() {
                    @Override
                    public List<JsonPrimitive> apply(ByteString bytes) throws Exception {
                        List<JsonPrimitive> jsonArray = new ArrayList<>();
                        Utils.Longs.parseFrom(bytes).getArrayList().forEach(l -> {
                            jsonArray.add(new JsonPrimitive(l));
                        });
                        return jsonArray;
                    }
                },
                new FunctionWithException<List<JsonPrimitive>, ByteString>() {
                    @Override
                    public ByteString apply(List<JsonPrimitive> jsonElements) throws Exception {
                        try {
                            Utils.Longs.Builder builder = Utils.Longs.newBuilder();
                            jsonElements.forEach(jsonElement -> builder.addArray(jsonElement.getAsLong()));
                            return builder.build().toByteString();
                        } catch (NumberFormatException e) {
                            throw new NumberFormatException(String.format("Error when convert to long value"));
                        }
                    }
                }
        ));

        convertersMap.put(DOUBLE_TYPE_STR, new AbstractMap.SimpleImmutableEntry(new Function<byte[], JsonPrimitive>() {
            @Override
            public JsonPrimitive apply(byte[] bytes) {
                return new JsonPrimitive(byteArrayToDouble(bytes));
            }
        }, new Function<JsonPrimitive, byte[]>() {
            @Override
            public byte[] apply(JsonPrimitive jsonPrimitive) {
                return doubleToBytes(jsonPrimitive.getAsDouble());
            }
        }));
        listConvertorsMap.put(DOUBLE_TYPE_STR, new AbstractMap.SimpleImmutableEntry(
                new FunctionWithException<ByteString, List<JsonPrimitive>>() {
                    @Override
                    public List<JsonPrimitive> apply(ByteString bytes) throws Exception {
                        List<JsonPrimitive> jsonArray = new ArrayList<>();
                        Utils.Doubles.parseFrom(bytes).getArrayList().forEach(d -> {
                            jsonArray.add(new JsonPrimitive(d));
                        });
                        return jsonArray;
                    }
                },
                new FunctionWithException<List<JsonPrimitive>, ByteString>() {
                    @Override
                    public ByteString apply(List<JsonPrimitive> jsonElements) throws Exception {
                        try {
                            Utils.Doubles.Builder builder = Utils.Doubles.newBuilder();
                            jsonElements.forEach(jsonElement -> builder.addArray(jsonElement.getAsDouble()));
                            return builder.build().toByteString();
                        } catch (NumberFormatException e) {
                            throw new NumberFormatException(String.format("Error when convert to double value"));
                        }
                    }
                }
        ));

        convertersMap.put(STRING_TYPE_STR, new AbstractMap.SimpleImmutableEntry(new Function<byte[], JsonPrimitive>() {
            @Override
            public JsonPrimitive apply(byte[] bytes) {
                return new JsonPrimitive(new String(bytes));
            }
        }, new Function<JsonPrimitive, byte[]>() {
            @Override
            public byte[] apply(JsonPrimitive jsonPrimitive) {
                return jsonPrimitive.getAsString().getBytes(StandardCharsets.UTF_8);
            }
        }));

        listConvertorsMap.put(STRING_TYPE_STR, new AbstractMap.SimpleImmutableEntry(
                new FunctionWithException<ByteString, List<JsonPrimitive>>() {
                    @Override
                    public List<JsonPrimitive> apply(ByteString bytes) throws Exception {
                        List<JsonPrimitive> jsonArray = new ArrayList<>();
                        Utils.Strings.parseFrom(bytes).getArrayList().forEach(s -> {
                            jsonArray.add(new JsonPrimitive(s));
                        });
                        return jsonArray;
                    }
                },
                new FunctionWithException<List<JsonPrimitive>, ByteString>() {
                    @Override
                    public ByteString apply(List<JsonPrimitive> jsonElements) throws Exception {
                        try {
                            Utils.Strings.Builder builder = Utils.Strings.newBuilder();
                            jsonElements.forEach(jsonElement -> builder.addArray(jsonElement.getAsString()));
                            return builder.build().toByteString();
                        } catch (NumberFormatException e) {
                            throw new NumberFormatException(String.format("Error when convert to string value"));
                        }
                    }
                }
        ));

        convertersMap.put(BOOLEAN_TYPE_STR, new AbstractMap.SimpleImmutableEntry(new Function<byte[], JsonPrimitive>() {
            @Override
            public JsonPrimitive apply(byte[] bytes) {
                return new JsonPrimitive(bytes[0] == 0 ? Boolean.FALSE : Boolean.TRUE);
            }
        }, new Function<JsonPrimitive, byte[]>() {
            @Override
            public byte[] apply(JsonPrimitive jsonPrimitive) {
                return new byte[]{(byte) (jsonPrimitive.getAsBoolean() ? 1 : 0)};
            }
        }));
        listConvertorsMap.put(BOOLEAN_TYPE_STR, new AbstractMap.SimpleImmutableEntry(
                new FunctionWithException<ByteString, List<JsonPrimitive>>() {
                    @Override
                    public List<JsonPrimitive> apply(ByteString bytes) throws Exception {
                        List<JsonPrimitive> jsonArray = new ArrayList<>();
                        Utils.Booleans.parseFrom(bytes).getArrayList().forEach(b -> {
                            jsonArray.add(new JsonPrimitive(b));
                        });
                        return jsonArray;
                    }
                },
                new FunctionWithException<List<JsonPrimitive>, ByteString>() {
                    @Override
                    public ByteString apply(List<JsonPrimitive> jsonElements) throws Exception {
                        try {
                            Utils.Booleans.Builder builder = Utils.Booleans.newBuilder();
                            jsonElements.forEach(jsonElement -> builder.addArray(jsonElement.getAsBoolean()));
                            return builder.build().toByteString();
                        } catch (NumberFormatException e) {
                            throw new NumberFormatException(String.format("Error when convert to boolean value"));
                        }
                    }
                }
        ));

        converters = ImmutableMap.copyOf(convertersMap);
        listConverters = ImmutableMap.copyOf(listConvertorsMap);
    }

    public static boolean isSupport(String type) {
        return converters.containsKey(type);
    }

    public static byte[] toByteArray(JsonPrimitive jsonPrimitive, String type) throws Exception {
        if (!converters.containsKey(type)) {
            throw new IllegalArgumentException(String.format("Type %s is not supported", type));
        }
        return converters.get(type).getValue().apply(jsonPrimitive);
    }

    public static List<JsonPrimitive> toListV(ByteString bytes, String type) throws Exception {
        if (!listConverters.containsKey(type)) {
            throw new UnsupportedOperationException(String.format("Type %s is not supported", type));
        }
        return listConverters.get(type).getKey().apply(bytes);
    }

    public static ByteString toByteArray(List<JsonPrimitive> jsonArray, String type) throws Exception {
        if (!listConverters.containsKey(type)) {
            throw new UnsupportedOperationException(String.format("Type %s is not supported", type));
        }
        return listConverters.get(type).getValue().apply(jsonArray);
    }

    public static byte[] doubleToBytes(double dblValue) {
        long data = Double.doubleToRawLongBits(dblValue);
        return new byte[]{
                (byte) ((data >> 56) & 0xff),
                (byte) ((data >> 48) & 0xff),
                (byte) ((data >> 40) & 0xff),
                (byte) ((data >> 32) & 0xff),
                (byte) ((data >> 24) & 0xff),
                (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff),
                (byte) ((data >> 0) & 0xff),
        };
    }

    public static double byteArrayToDouble(byte[] data) {
        if (data == null || data.length % Double.BYTES != 0) return Double.NaN;
        // ----------

        return (byteArrayToDouble(new byte[]{
                        data[(Double.BYTES)],
                        data[(Double.BYTES) + 1],
                        data[(Double.BYTES) + 2],
                        data[(Double.BYTES) + 3],
                        data[(Double.BYTES) + 4],
                        data[(Double.BYTES) + 5],
                        data[(Double.BYTES) + 6],
                        data[(Double.BYTES) + 7],
                }
        ));
    }

}
