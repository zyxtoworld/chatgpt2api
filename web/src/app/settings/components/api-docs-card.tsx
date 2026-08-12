"use client";

import { ChevronDown, FileArchive, FileText, KeyRound, ListChecks, type LucideIcon } from "lucide-react";

import { Card, CardContent } from "@/components/ui/card";
import webConfig from "@/constants/common-env";

const API_KEY_PLACEHOLDER = "<YOUR_API_KEY>";

type ParamRow = [string, string, string];

type ApiDoc = {
  title: string;
  method: string;
  path: string;
  icon: LucideIcon;
  input: ParamRow[];
  output: ParamRow[];
  example: (baseUrl: string, key: string) => string;
};

const docs: ApiDoc[] = [
  {
    title: "模型列表",
    method: "GET",
    path: "/v1/models",
    icon: ListChecks,
    input: [
      ["Authorization", "header", "Bearer <auth-key>。"],
    ],
    output: [
      ["data", "array", "模型列表；上游公布时包含 supported_reasoning_efforts。"],
    ],
    example: (baseUrl: string, key: string) => `curl ${baseUrl}/models \\
  -H "Authorization: Bearer ${key}"`,
  },
  {
    title: "聊天补全",
    method: "POST",
    path: "/v1/chat/completions",
    icon: FileText,
    input: [
      ["model", "string", "模型名，例如 gpt-5-mini，也可用于图片兼容场景。"],
      ["messages", "array", "支持文本、图片、developer 与 wav/mp3 input_audio；消息级 name 和未知字段会返回 400。"],
      ["stream", "boolean", "可选，是否流式返回。"],
      ["stream_options", "object", "流式请求可用 include_usage 返回终态 usage chunk，并支持 include_obfuscation=false。"],
      ["reasoning_effort / thinking_effort", "string", "按所选模型上游能力归一化；不支持的值回退到该模型最强档。"],
      ["tools", "array", "支持官方嵌套 function 定义；工具结果续轮可仅提交历史 tool_calls / tool 消息。"],
      ["tool_choice", "string", "固定 Codex 上游当前仅支持 auto；省略时同样使用 auto。"],
      ["parallel_tool_calls", "boolean", "原生工具链原值透传；省略时默认 true。"],
      ["web_search_options", "object", "可选，映射为 Codex 原生 web_search 工具。"],
      ["response_format", "object", "支持 text 与可映射的 json_schema；无需同时声明工具。"],
      ["verbosity", "string", "可选，low、medium 或 high，映射到 Codex Responses。"],
      ["prompt_cache_key / service_tier", "string", "可选，传入时使用 Codex Responses 原生链路。"],
      ["n", "number", "可选，图片兼容场景会解析为生成数量。"],
    ],
    output: [
      ["id", "string", "响应 ID。"],
      ["choices", "array", "OpenAI 兼容 choices。"],
      ["usage", "object", "可选，token 使用信息。"],
    ],
    example: (baseUrl: string, key: string) => `curl ${baseUrl}/chat/completions \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer ${key}" \\
  -d '{"model":"gpt-5-mini","messages":[{"role":"user","content":"你好"}]}'`,
  },
  {
    title: "Responses",
    method: "POST",
    path: "/v1/responses",
    icon: FileText,
    input: [
      ["model", "string", "Responses 编排模型；图片模型由 image_generation.model 指定，默认 gpt-image-2。"],
      ["input", "string | array | object", "用户输入；EasyInputMessage 可省略 type，支持 user/assistant/system/developer，phase 仅用于 assistant；原生历史 item 严格校验。顶层多模态 part 会规范化为用户 message；input_file、file_id、caller、content part 缓存断点及未知/畸形 item/part 返回 400。"],
      ["instructions", "string", "可选顶层指令；文本及 Codex 原生工具链会透传给上游，图片工具请求传入时返回 400。"],
      ["context_management", "array", "Codex 文本/原生工具支持 compaction；compact_threshold 至少为 1000。"],
      ["tools", "array", "支持扁平 function、web_search 和 image_generation；工具输出续轮无需重复定义 tools，函数/自定义工具输出支持字符串或固定 Codex 可表达的文本、图片、音频、加密内容数组。"],
      ["web_search 控制", "object", "支持上下文大小、位置、最多 100 个无协议前缀的 allowed_domains、外网访问开关及 text/image 搜索类型。"],
      ["include", "array", "支持返回 web_search_call.action.sources 与 web_search_call.results。"],
      ["tool_choice", "string", "固定 Codex 上游当前仅支持 auto；其他值返回 400。"],
      ["parallel_tool_calls", "boolean", "原生工具链原值透传；省略时默认 true。"],
      ["reasoning", "object", "effort 按模型能力归一化；summary/context 单独传入即可触发 Codex 原生链。"],
      ["text", "object", "支持 verbosity 与可映射的 json_schema；无需同时声明工具。"],
      ["prompt_cache_key / service_tier", "string", "单独传入即可使用 Codex Responses 原生链。"],
      ["stream", "boolean", "可选，是否流式返回。"],
      ["stream_options", "object", "支持 include_obfuscation=false；reasoning_summary_delivery=sequential_cutoff 走原生链。"],
    ],
    output: [
      ["id", "string", "响应 ID。"],
      ["output", "array", "Responses 兼容输出。"],
      ["status", "string", "响应状态。"],
    ],
    example: (baseUrl: string, key: string) => `curl ${baseUrl}/responses \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer ${key}" \\
  -d '{"model":"gpt-5-mini","input":"生成一张未来城市图片","tools":[{"type":"image_generation","model":"gpt-image-2"}]}'`,
  },
  {
    title: "搜索",
    method: "POST",
    path: "/v1/search",
    icon: ListChecks,
    input: [
      ["prompt", "string", "搜索问题或检索指令。"],
    ],
    output: [
      ["answer", "string", "搜索后的回答内容，具体字段以返回结果为准。"],
      ["sources", "array", "可选，搜索引用来源。"],
    ],
    example: (baseUrl: string, key: string) => `curl ${baseUrl}/search \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer ${key}" \\
  -d '{"prompt":"搜索 chatgpt2api 最新使用方式"}'`,
  },
  {
    title: "图片生成",
    method: "POST",
    path: "/v1/images/generations",
    icon: FileArchive,
    input: [
      ["prompt", "string", "图片生成提示词。"],
      ["model", "string", "可选，默认 gpt-image-2。"],
      ["n", "number", "可选，生成数量，官方范围 1-10。"],
      ["size", "string", "可选，auto、标准尺寸或合法 WIDTHxHEIGHT；gpt-image-2 最大支持 3840x2160 边界。"],
      ["quality", "string", "可选，auto（默认）、low、medium 或 high。"],
      ["response_format", "string", "可选，默认 b64_json。"],
      ["output_format", "string", "可选，png（默认）、jpeg 或 webp。"],
      ["output_compression", "number", "可选，jpeg/webp 压缩质量，范围 0-100。"],
      ["stream", "boolean", "可选，返回 image_generation.completed 类型化 SSE，不发送 [DONE]。"],
      ["partial_images", "number", "当前只接受省略或 0；上游没有真实局部位图能力。"],
      ["background / moderation", "string", "当前上游只支持 auto；其他值明确返回 400。"],
    ],
    output: [
      ["data", "array", "图片结果列表。"],
      ["data[].b64_json", "string", "base64 图片内容。"],
      ["data[].url", "string", "部分配置下返回图片 URL。"],
      ["image_generation.completed", "SSE event", "流式完成事件，包含 b64_json、输出元数据与 usage。"],
    ],
    example: (baseUrl: string, key: string) => `curl ${baseUrl}/images/generations \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer ${key}" \\
  -d '{"model":"gpt-image-2","prompt":"一张极简产品海报","n":1}'`,
  },
  {
    title: "图片编辑",
    method: "POST",
    path: "/v1/images/edits",
    icon: FileArchive,
    input: [
      ["image / images", "file | file[] | reference[]", "参考图，支持上传、data URL 或经 SSRF 校验的公开 HTTP(S) URL，最多 16 张。"],
      ["mask", "file | reference", "可选单张遮罩；须与第一张输入图格式、尺寸一致并包含 alpha 通道。"],
      ["prompt", "string", "编辑提示词。"],
      ["model", "string", "可选，默认 gpt-image-2。"],
      ["n", "number", "可选，生成数量，官方范围 1-10。"],
      ["size", "string", "可选，auto、1024x1024、1536x1024 或 1024x1536。"],
      ["quality", "string", "可选，auto（默认）、low、medium 或 high。"],
      ["response_format", "string", "可选，b64_json（默认）或 url；流式请求只支持 b64_json。"],
      ["output_format", "string", "可选，png（默认）、jpeg 或 webp。"],
      ["output_compression", "number", "可选，jpeg/webp 压缩质量，范围 0-100。"],
      ["stream", "boolean", "可选，返回 image_edit.completed 类型化 SSE，不发送 [DONE]。"],
      ["input_fidelity", "string", "当前上游不支持，传入时明确返回 400。"],
    ],
    output: [
      ["data", "array", "编辑后的图片结果列表。"],
      ["data[].b64_json", "string", "base64 图片内容。"],
      ["data[].url", "string", "部分配置下返回图片 URL。"],
      ["image_edit.completed", "SSE event", "流式完成事件，包含 b64_json、输出元数据与 usage。"],
    ],
    example: (baseUrl: string, key: string) => `curl ${baseUrl}/images/edits \\
  -H "Authorization: Bearer ${key}" \\
  -F "model=gpt-image-2" \\
  -F "prompt=改成赛博朋克夜景" \\
  -F "image=@./input.png"`,
  },
  {
    title: "创建 PPT 任务",
    method: "POST",
    path: "/v1/ppt/generations",
    icon: FileText,
    input: [
      ["prompt", "string", "PPT 需求描述，可为空但建议填写完整主题、页数、风格和内容结构。"],
      ["base64_images", "string[]", "可选，图片 data URL/base64，用作 PPT 参考素材。"],
      ["client_task_id", "string", "可选，客户端幂等任务 ID；重复提交同 ID 会返回已有任务。"],
    ],
    output: [
      ["id / taskId", "string", "任务 ID，用于轮询状态。"],
      ["status", "queued | running | success | error", "任务状态。"],
      ["kind", "ppt", "任务类型。"],
      ["created_at / updated_at", "string", "任务创建和更新时间。"],
    ],
    example: (baseUrl: string, key: string) => `curl ${baseUrl}/ppt/generations \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer ${key}" \\
  -d '{"prompt":"制作一份 8 页以内的季度业务汇报 PPT","base64_images":[]}'`,
  },
  {
    title: "创建 PSD 任务",
    method: "POST",
    path: "/v1/psd/generations",
    icon: FileArchive,
    input: [
      ["prompt", "string", "PSD 拆分与合成要求，例如保留图层、位置、背景和素材 zip。"],
      ["base64_images", "string[]", "必填，至少一张图片 data URL/base64，作为 PSD 拆分源图。"],
      ["client_task_id", "string", "可选，客户端幂等任务 ID。"],
    ],
    output: [
      ["id / taskId", "string", "任务 ID，用于轮询状态。"],
      ["status", "queued | running | success | error", "任务状态。"],
      ["kind", "psd", "任务类型。"],
      ["error", "string", "失败时返回错误信息。"],
    ],
    example: (baseUrl: string, key: string) => `curl ${baseUrl}/psd/generations \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer ${key}" \\
  -d '{"prompt":"按原图位置拆分海报元素并合成可编辑 PSD","base64_images":["data:image/png;base64,..."]}'`,
  },
  {
    title: "任务状态查询",
    method: "GET",
    path: "/v1/editable-file-tasks?ids={taskId1,taskId2}",
    icon: ListChecks,
    input: [
      ["ids", "string", "可选，逗号分隔任务 ID；不传则返回当前用户全部可编辑文件任务。"],
    ],
    output: [
      ["items", "array", "任务列表。成功任务的 result 内包含 primary_url 和 zip_url。"],
      ["missing_ids", "string[]", "查询指定 ids 时，返回未找到的任务 ID。"],
      ["result.primary_url", "string", "主文件下载地址。"],
      ["result.zip_url", "string", "素材 zip 下载地址。"],
    ],
    example: (baseUrl: string, key: string) => `curl "${baseUrl}/editable-file-tasks?ids=<task_id>" \\
  -H "Authorization: Bearer ${key}"`,
  },
  {
    title: "结果文件下载",
    method: "GET",
    path: "/files/{file_path}",
    icon: FileArchive,
    input: [
      ["file_path", "string", "由任务 result.primary_url 或 result.zip_url 返回，通常不需要手动拼接。"],
    ],
    output: [
      ["binary", "file", "返回 pptx/psd/zip 文件流。"],
    ],
    example: (baseUrl: string, _key: string) => `curl ${baseUrl.replace(/\/v1$/, "")}/files/<file_path> -o result.zip`,
  },
];

const usableModels = ["gpt-image-2", "codex-gpt-image-2", "auto", "gpt-5", "gpt-5-1", "gpt-5-2", "gpt-5-3", "gpt-5-3-mini", "gpt-5-mini"];

function ParamTable({ rows }: { rows: ParamRow[] }) {
  return (
    <div className="overflow-hidden rounded-lg border border-stone-200">
      <table className="w-full text-left text-xs">
        <thead className="bg-stone-50 text-stone-500">
          <tr>
            <th className="px-3 py-2 font-medium">参数</th>
            <th className="px-3 py-2 font-medium">类型</th>
            <th className="px-3 py-2 font-medium">说明</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-stone-100 bg-white">
          {rows.map(([name, type, desc]) => (
            <tr key={name}>
              <td className="px-3 py-2 font-mono text-stone-800">{name}</td>
              <td className="px-3 py-2 font-mono text-stone-500">{type}</td>
              <td className="px-3 py-2 text-stone-600">{desc}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function ApiDocsCard() {
  const serviceBaseUrl = webConfig.apiUrl.replace(/\/$/, "") || (typeof window !== "undefined" ? window.location.origin : "");
  const openAIBaseUrl = `${serviceBaseUrl}/v1`;
  const displayKey = API_KEY_PLACEHOLDER;

  return (
    <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
      <CardContent className="space-y-5 p-6">
        <div>
          <div className="flex items-center gap-2 text-base font-semibold text-stone-900">
            <KeyRound className="size-5 text-stone-500" />
            接口接入说明
          </div>
          <p className="mt-1 text-xs leading-6 text-stone-500">
            第三方应用按 OpenAI 兼容接口接入；文件任务接口也使用同一套鉴权方式。
          </p>
        </div>

        <div className="grid gap-3 md:grid-cols-2">
          <div className="space-y-1 rounded-xl border border-stone-200 bg-white px-3 py-2">
            <div className="text-xs text-stone-500">服务地址</div>
            <div className="break-all font-mono text-xs text-stone-800">{serviceBaseUrl}</div>
          </div>
          <div className="space-y-1 rounded-xl border border-stone-200 bg-white px-3 py-2">
            <div className="text-xs text-stone-500">Base URL（OpenAI）</div>
            <div className="break-all font-mono text-xs text-stone-800">{openAIBaseUrl}</div>
          </div>
          <div className="space-y-1 rounded-xl border border-stone-200 bg-white px-3 py-2">
            <div className="text-xs text-stone-500">API Key</div>
            <div className="break-all font-mono text-xs text-stone-800">{displayKey}</div>
          </div>
          <div className="space-y-1 rounded-xl border border-stone-200 bg-white px-3 py-2">
            <div className="text-xs text-stone-500">请求头</div>
            <div className="break-all font-mono text-xs text-stone-800">Authorization: Bearer {displayKey}</div>
          </div>
        </div>

        <div className="space-y-2">
          <div className="text-xs font-medium text-stone-600">常用模型，也可请求 /v1/models 获取</div>
          <div className="flex flex-wrap gap-2">
            {usableModels.map((model) => (
              <span key={model} className="rounded-md border border-stone-200 bg-white px-2 py-1 font-mono text-xs text-stone-700">{model}</span>
            ))}
          </div>
        </div>

        <div className="space-y-3">
          {docs.map((item) => {
            const Icon = item.icon;
            return (
              <details key={item.path} className="group rounded-xl border border-stone-200 bg-white px-4 py-3">
                <summary className="flex cursor-pointer list-none items-center justify-between gap-3">
                  <span className="flex min-w-0 items-center gap-3">
                    <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-stone-100 text-stone-600">
                      <Icon className="size-4" />
                    </span>
                    <span className="min-w-0">
                      <span className="block text-sm font-semibold text-stone-900">{item.title}</span>
                      <span className="mt-1 block truncate font-mono text-xs text-stone-500">{item.method} {item.path}</span>
                    </span>
                  </span>
                  <ChevronDown className="size-4 shrink-0 text-stone-400 transition group-open:rotate-180" />
                </summary>

                <div className="mt-4 grid gap-4 lg:grid-cols-2">
                  <div className="space-y-2">
                    <h3 className="text-xs font-semibold text-stone-700">输入参数</h3>
                    <ParamTable rows={item.input} />
                  </div>
                  <div className="space-y-2">
                    <h3 className="text-xs font-semibold text-stone-700">输出参数</h3>
                    <ParamTable rows={item.output} />
                  </div>
                  <div className="space-y-2 lg:col-span-2">
                    <h3 className="text-xs font-semibold text-stone-700">调用示例</h3>
                    <pre className="overflow-auto whitespace-pre-wrap break-all rounded-xl bg-stone-950 px-3 py-3 text-xs leading-5 text-stone-100">{item.example(openAIBaseUrl, displayKey)}</pre>
                  </div>
                </div>
              </details>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
