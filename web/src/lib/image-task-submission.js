export async function settleImageTaskSubmissions(images, submit) {
  const outcomes = await Promise.all(
    images.map(async (image) => {
      try {
        return { task: await submit(image) };
      } catch (error) {
        return {
          failure: {
            imageId: String(image.id),
            error,
            message: error instanceof Error ? error.message : "图片任务提交失败",
          },
        };
      }
    }),
  );

  return {
    tasks: outcomes.flatMap((outcome) => (outcome.task ? [outcome.task] : [])),
    failures: outcomes.flatMap((outcome) => (outcome.failure ? [outcome.failure] : [])),
  };
}
