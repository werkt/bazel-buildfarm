load("@protobuf//bazel/common:proto_common.bzl", "proto_common")
load("@protobuf//bazel/private:toolchain_helpers.bzl", "toolchains")

JsProtoAspectInfo = provider("JsProtoAspectInfo", fields = ["srcs"])

_JS_PROTO_TOOLCHAIN = "//src/main/protobuf/build/buildfarm/v1test:js_toolchain_type"

def _bazel_js_proto_aspect_impl(target, ctx):
    source_js = None
    # Generate source js using proto compiler.
    proto_toolchain_info = toolchains.find_toolchain(ctx, "_aspect_js_proto_toolchain", _JS_PROTO_TOOLCHAIN)
    source_js = ctx.actions.declare_file(ctx.label.name + ".js")
    proto_common.compile(
        ctx.actions,
        target[ProtoInfo],
        proto_toolchain_info,
        [source_js],
        experimental_output_files = proto_toolchain_info.output_files,
    )

    # Compile Java sources (or just merge if there aren't any)
    #deps = _filter_provider(JavaInfo, ctx.rule.attr.deps)
    #exports = _filter_provider(JavaInfo, ctx.rule.attr.exports)
    #if source_jar and proto_toolchain_info.runtime:
    #    deps.append(proto_toolchain_info.runtime[JavaInfo])
    #js_info, jars = js_compile_for_protos(
    #    ctx,
    #    "-speed.jar",
    #    source_jar,
    #    deps,
    #    exports,
    #)

    transitive_srcs = [dep[JsProtoAspectInfo].srcs for dep in ctx.rule.attr.deps if JsProtoAspectInfo in dep]
    return [
        # js_info,
        JsProtoAspectInfo(srcs = depset([source_js], transitive = transitive_srcs)),
    ]

bazel_js_proto_aspect = aspect(
    implementation = _bazel_js_proto_aspect_impl,
    attrs = toolchains.if_legacy_toolchain({
        "_aspect_js_proto_toolchain": attr.label(
            default = "//src/main/protobuf/build/buildfarm/v1test:js_toolchain",
        ),
    }),
    attr_aspects = ["deps", "exports"],
    required_providers = [ProtoInfo],
    provides = [
        # JavaInfo,
        JsProtoAspectInfo,
    ],
    fragments = ["js"],
)

def _js_proto_library(ctx):
    proto_toolchain = None # toolchains.find_toolchain(ctx, "_aspect_js_proto_toolchain", _JS_PROTO_TOOLCHAIN)
    for dep in ctx.attr.deps:
        proto_common.check_collocated(ctx.label, dep[ProtoInfo], proto_toolchain)

    transitive_src = depset(transitive = [dep[JsProtoAspectInfo].srcs for dep in ctx.attr.deps])
    return [
        # js_info,
        DefaultInfo(
            files = transitive_src,
        ),
        OutputGroupInfo(default = depset()),
    ]


js_proto_library = rule(
    implementation = _js_proto_library,
    attrs = {
        "plugin": attr.label(default="@gonzojive_protobuf_javascript//generator:protoc-gen-js"),
        "deps": attr.label_list(
            providers = [ProtoInfo],
            aspects = [bazel_js_proto_aspect],
            doc = """
The list of <a href="protocol-buffer.html#proto_library"><code>proto_library</code></a>
rules to generate Javascript code for.
            """,
        ),
        # buildifier: disable=attr-license (calling attr.license())
        "licenses": attr.license() if hasattr(attr, "license") else attr.string_list(),
        "distribs": attr.string_list(),
    },
)
