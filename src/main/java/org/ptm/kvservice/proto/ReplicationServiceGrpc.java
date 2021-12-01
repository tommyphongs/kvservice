package org.ptm.kvservice.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.9.1)",
    comments = "Source: ReplicationService.proto")
public final class ReplicationServiceGrpc {

  private ReplicationServiceGrpc() {}

  public static final String SERVICE_NAME = "proto.ReplicationService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getSyncMethod()} instead. 
  public static final io.grpc.MethodDescriptor<SequenceNumber,
          Updates> METHOD_SYNC = getSyncMethod();

  private static volatile io.grpc.MethodDescriptor<SequenceNumber,
          Updates> getSyncMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<SequenceNumber,
          Updates> getSyncMethod() {
    io.grpc.MethodDescriptor<SequenceNumber, Updates> getSyncMethod;
    if ((getSyncMethod = ReplicationServiceGrpc.getSyncMethod) == null) {
      synchronized (ReplicationServiceGrpc.class) {
        if ((getSyncMethod = ReplicationServiceGrpc.getSyncMethod) == null) {
          ReplicationServiceGrpc.getSyncMethod = getSyncMethod = 
              io.grpc.MethodDescriptor.<SequenceNumber, Updates>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "proto.ReplicationService", "sync"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  SequenceNumber.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Updates.getDefaultInstance()))
                  .setSchemaDescriptor(new ReplicationServiceMethodDescriptorSupplier("sync"))
                  .build();
          }
        }
     }
     return getSyncMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ReplicationServiceStub newStub(io.grpc.Channel channel) {
    return new ReplicationServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ReplicationServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ReplicationServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ReplicationServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ReplicationServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class ReplicationServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void sync(SequenceNumber request,
                     io.grpc.stub.StreamObserver<Updates> responseObserver) {
      asyncUnimplementedUnaryCall(getSyncMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSyncMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                      SequenceNumber,
                      Updates>(
                  this, METHODID_SYNC)))
          .build();
    }
  }

  /**
   */
  public static final class ReplicationServiceStub extends io.grpc.stub.AbstractStub<ReplicationServiceStub> {
    private ReplicationServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ReplicationServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ReplicationServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ReplicationServiceStub(channel, callOptions);
    }

    /**
     */
    public void sync(SequenceNumber request,
                     io.grpc.stub.StreamObserver<Updates> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getSyncMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ReplicationServiceBlockingStub extends io.grpc.stub.AbstractStub<ReplicationServiceBlockingStub> {
    private ReplicationServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ReplicationServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ReplicationServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ReplicationServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<Updates> sync(
        SequenceNumber request) {
      return blockingServerStreamingCall(
          getChannel(), getSyncMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ReplicationServiceFutureStub extends io.grpc.stub.AbstractStub<ReplicationServiceFutureStub> {
    private ReplicationServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ReplicationServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ReplicationServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ReplicationServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_SYNC = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ReplicationServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ReplicationServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SYNC:
          serviceImpl.sync((SequenceNumber) request,
              (io.grpc.stub.StreamObserver<Updates>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class ReplicationServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ReplicationServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return ReplicationServiceOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ReplicationService");
    }
  }

  private static final class ReplicationServiceFileDescriptorSupplier
      extends ReplicationServiceBaseDescriptorSupplier {
    ReplicationServiceFileDescriptorSupplier() {}
  }

  private static final class ReplicationServiceMethodDescriptorSupplier
      extends ReplicationServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ReplicationServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ReplicationServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ReplicationServiceFileDescriptorSupplier())
              .addMethod(getSyncMethod())
              .build();
        }
      }
    }
    return result;
  }
}
