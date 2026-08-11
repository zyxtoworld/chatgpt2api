from __future__ import annotations

from io import BytesIO
from typing import Any, Iterator

from PIL import Image

from services.protocol.conversation import (
    ConversationRequest,
    ImageGenerationError,
    collect_image_outputs,
    count_text_tokens,
    encode_images,
    stream_image_events,
    stream_image_outputs_with_pool,
)
from utils.image_tokens import count_image_inputs_tokens, count_image_output_items_tokens, image_usage


def _invalid_mask_error() -> ImageGenerationError:
    return ImageGenerationError(
        "invalid image edit mask",
        status_code=400,
        error_type="invalid_request_error",
        code="invalid_image_mask",
        param="mask",
    )


def _composite_mask(
    images: list[tuple[bytes, str, str]],
    masks: list[tuple[bytes, str, str]],
) -> list[tuple[bytes, str, str]]:
    """将 mask 的 alpha 通道合成到图片中，标识需要编辑的区域。
    
    mask 的透明区域（低 alpha）= 需要编辑的区域，
    mask 的不透明区域（高 alpha）= 保留的区域。
    如果无 mask 则返回原图。
    """
    if not masks:
        return images
    if len(masks) != 1 or not images:
        raise _invalid_mask_error()

    data, filename, _mime_type = images[0]
    mask_data = masks[0][0]
    try:
        with Image.open(BytesIO(data)) as source_image:
            source_format = source_image.format
            source_size = source_image.size
            image = source_image.convert("RGBA")
        with Image.open(BytesIO(mask_data)) as mask_image:
            mask_format = mask_image.format
            mask_size = mask_image.size
            mask_bands = mask_image.getbands()
            mask_image.load()
            alpha = mask_image.getchannel("A").copy() if "A" in mask_bands else None
    except (OSError, ValueError, Image.DecompressionBombError) as exc:
        raise _invalid_mask_error() from exc

    if not source_format or source_format != mask_format or source_size != mask_size or alpha is None:
        raise _invalid_mask_error()

    image.putalpha(alpha)
    buf = BytesIO()
    image.save(buf, format="PNG")
    result = list(images)
    result[0] = (buf.getvalue(), filename, "image/png")
    return result


def handle(body: dict[str, Any]) -> dict[str, Any] | Iterator[dict[str, Any]]:
    prompt = str(body.get("prompt") or "")
    images = body.get("images") or []
    masks = body.get("mask") or []
    images = _composite_mask(images, masks)
    model = str(body.get("model") or "gpt-image-2")
    n = int(body.get("n") or 1)
    size = body.get("size")
    quality = str(body.get("quality") or "auto")
    response_format = str(body.get("response_format") or "b64_json")
    output_format = str(body.get("output_format") or "png")
    output_compression = body.get("output_compression")
    background = str(body.get("background") or "auto")
    base_url = str(body.get("base_url") or "") or None
    progress_callback = body.get("progress_callback")
    encoded_images = encode_images(images)
    if not encoded_images:
        raise ImageGenerationError("image is required")
    outputs = stream_image_outputs_with_pool(ConversationRequest(
        prompt=prompt,
        model=model,
        n=n,
        size=size,
        quality=quality,
        response_format=response_format,
        output_format=output_format,
        output_compression=output_compression,
        background=background,
        base_url=base_url,
        images=encoded_images,
        message_as_error=True,
        progress_callback=progress_callback,
    ))
    if body.get("stream"):
        return stream_image_events(
            outputs,
            lambda items: image_usage(
                input_text_tokens=count_text_tokens(prompt, model),
                input_image_tokens=count_image_inputs_tokens(images, model),
                output_tokens=count_image_output_items_tokens(items, size, quality),
            ),
            event_type="image_edit.completed",
            background=background,
            output_format=output_format,
            quality=quality,
            size=size or "auto",
        )
    result = collect_image_outputs(outputs)
    result["usage"] = image_usage(
        input_text_tokens=count_text_tokens(prompt, model),
        input_image_tokens=count_image_inputs_tokens(images, model),
        output_tokens=count_image_output_items_tokens(result.get("data"), size, quality),
    )
    result.update({
        "background": background,
        "output_format": output_format,
        "quality": quality,
    })
    if size:
        result["size"] = size
    return result
