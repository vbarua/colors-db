package io.substrait.colors;

import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import java.util.List;

public class ColorsProtobufUtils {

  public static final List<Descriptors.Descriptor> EXTENSIONS =
      List.of(io.substrait.colors.extensions.protobuf.RedQuery.getDescriptor());

  public static final JsonFormat.TypeRegistry TYPE_REGISTRY =
      JsonFormat.TypeRegistry.newBuilder().add(EXTENSIONS).build();
}
