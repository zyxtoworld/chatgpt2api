export function getStoredImageSrc(image) {
  if (image?.b64_json) {
    return `data:image/png;base64,${image.b64_json}`;
  }
  return image?.url || "";
}
