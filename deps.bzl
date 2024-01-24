"""
buildfarm dependencies that can be imported into other WORKSPACE files
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def archive_dependencies(third_party):
    return [
        # Needed for "well-known protos" and @com_google_protobuf//:protoc.
        {
            "name": "com_google_protobuf",
            "sha256": "79082dc68d8bab2283568ce0be3982b73e19ddd647c2411d1977ca5282d2d6b3",
            "strip_prefix": "protobuf-25.0",
            "urls": ["https://github.com/protocolbuffers/protobuf/archive/v25.0.zip"],
        },
        # Needed for @grpc_java//compiler:grpc_java_plugin.
        {
            "name": "io_grpc_grpc_java",
            "sha256": "b8fb7ae4824fb5a5ae6e6fa26ffe2ad7ab48406fdeee54e8965a3b5948dd957e",
            "strip_prefix": "grpc-java-1.56.1",
            "urls": ["https://github.com/grpc/grpc-java/archive/v1.56.1.zip"],
            # Bzlmod: Waiting for https://github.com/bazelbuild/bazel-central-registry/issues/353
        },
        {
            "name": "rules_pkg",
            "sha256": "335632735e625d408870ec3e361e192e99ef7462315caa887417f4d88c4c8fb8",
            "urls": [
                "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.9.0/rules_pkg-0.9.0.tar.gz",
                "https://github.com/bazelbuild/rules_pkg/releases/download/0.9.0/rules_pkg-0.9.0.tar.gz",
            ],
            # Bzlmod : Waiting for >0.9.1 for https://github.com/bazelbuild/rules_pkg/pull/766 to be released
        },

        # The APIs that we implement.
        {
            "name": "googleapis",
            "build_file": "%s:BUILD.googleapis" % third_party,
            "patch_cmds": ["find google -name 'BUILD.bazel' -type f -delete"],
            "patch_cmds_win": ["Remove-Item google -Recurse -Include *.bazel"],
            "sha256": "745cb3c2e538e33a07e2e467a15228ccbecadc1337239f6740d57a74d9cdef81",
            "strip_prefix": "googleapis-6598bb829c9e9a534be674649ffd1b4671a821f9",
            "url": "https://github.com/googleapis/googleapis/archive/6598bb829c9e9a534be674649ffd1b4671a821f9.zip",
        },
        {
            "name": "remote_apis",
            "build_file": "%s:BUILD.remote_apis" % third_party,
            "patch_args": ["-p1"],
            "patches": ["%s/remote-apis:remote-apis.patch" % third_party],
            "sha256": "743d2d5b5504029f3f825beb869ce0ec2330b647b3ee465a4f39ca82df83f8bf",
            "strip_prefix": "remote-apis-636121a32fa7b9114311374e4786597d8e7a69f3",
            "url": "https://github.com/bazelbuild/remote-apis/archive/636121a32fa7b9114311374e4786597d8e7a69f3.zip",
        },

        # Used to format proto files
        {
            "name": "com_grail_bazel_toolchain",
            "sha256": "95f0bab6982c7e5a83447e08bf32fa7a47f210169da5e5ec62411fef0d8e7f59",
            "strip_prefix": "bazel-toolchain-0.9",
            "url": "https://github.com/grailbio/bazel-toolchain/archive/refs/tags/0.9.tar.gz",
            "patch_args": ["-p1"],
            "patches": ["%s:clang_toolchain.patch" % third_party],
        },

        # Used to build release container images
        {
            "name": "io_bazel_rules_docker",
            "sha256": "b1e80761a8a8243d03ebca8845e9cc1ba6c82ce7c5179ce2b295cd36f7e394bf",
            "urls": ["https://github.com/bazelbuild/rules_docker/releases/download/v0.25.0/rules_docker-v0.25.0.tar.gz"],
            "patch_args": ["-p0"],
            "patches": ["%s:docker_go_toolchain.patch" % third_party],
        },
        # Bazel is referenced as a dependency so that buildfarm can access the linux-sandbox as a potential execution wrapper.
        {
            "name": "bazel",
            "sha256": "06d3dbcba2286d45fc6479a87ccc649055821fc6da0c3c6801e73da780068397",
            "strip_prefix": "bazel-6.0.0",
            "urls": ["https://github.com/bazelbuild/bazel/archive/refs/tags/6.0.0.tar.gz"],
            "patch_args": ["-p1"],
            "patches": ["%s/bazel:bazel_visibility.patch" % third_party],
        },

        # Optional execution wrappers
        {
            "name": "skip_sleep",
            "build_file": "%s:BUILD.skip_sleep" % third_party,
            "sha256": "03980702e8e9b757df68aa26493ca4e8573770f15dd8a6684de728b9cb8549f1",
            "strip_prefix": "TARDIS-f54fa4743e67763bb1ad77039b3d15be64e2e564",
            "url": "https://github.com/Unilang/TARDIS/archive/f54fa4743e67763bb1ad77039b3d15be64e2e564.zip",
        },
        {
            "name": "rules_oss_audit",
            "sha256": "8ee8376b05b5ddd2287b070e9a88ec85ef907d47f44e321ce5d4bc2b192eed4e",
            "strip_prefix": "rules_oss_audit-167dab5b16abdb5996438f22364de544ff24693f",
            "url": "https://github.com/vmware/rules_oss_audit/archive/167dab5b16abdb5996438f22364de544ff24693f.zip",
            "patch_args": ["-p1"],
            "patches": ["%s:rules_oss_audit_pyyaml.patch" % third_party],
        },
    ]

def buildfarm_dependencies(repository_name = "build_buildfarm"):
    """
    Define all 3rd party archive rules for buildfarm

    Args:
      repository_name: the name of the repository
    """
    third_party = "@%s//third_party" % repository_name
    for dependency in archive_dependencies(third_party):
        params = {}
        params.update(**dependency)
        name = params.pop("name")
        maybe(http_archive, name, **params)

    maybe(
        http_jar,
        "opentelemetry",
        sha256 = "eccd069da36031667e5698705a6838d173d527a5affce6cc514a14da9dbf57d7",
        urls = [
            "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.28.0/opentelemetry-javaagent.jar",
        ],
    )

    http_file(
        name = "tini",
        sha256 = "12d20136605531b09a2c2dac02ccee85e1b874eb322ef6baf7561cd93f93c855",
        urls = ["https://github.com/krallin/tini/releases/download/v0.18.0/tini"],
    )
