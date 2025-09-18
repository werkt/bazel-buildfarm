const {GetCapabilitiesRequest, ServerCapabilities} = require('build/bazel/remote/execution/v2/remote_execution_pb.js');
const {CapabilitiesClient} = require('build/bazel/remote/execution/v2/remote_execution_grpc_web_pb.js');

var client = new CapabilitiesClient();

var request = new GetCapabilitiesRequest();
request.setInstanceName("shard");

client.getCapabilities(request, {}, (err, response) => {
  console.log(response);
}
