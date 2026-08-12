function deriveImageTags(items) {
  const seen = new Set();
  const tags = [];
  for (const item of items) {
    for (const tag of item.tags || []) {
      if (!seen.has(tag)) {
        seen.add(tag);
        tags.push(tag);
      }
    }
  }
  return tags;
}

export async function loadManagedImagesWithTags(fetchImages, fetchTags) {
  const imagesPromise = fetchImages();
  const tagsPromise = Promise.resolve()
    .then(fetchTags)
    .catch(() => null);
  const data = await imagesPromise;
  const tagsData = await tagsPromise;
  return {
    data,
    tags: tagsData ? tagsData.tags : deriveImageTags(data.items),
  };
}
