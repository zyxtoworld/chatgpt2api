export const PUBLIC_SERVER_ERROR_MESSAGE = "服务暂时不可用，请稍后重试";
export const PUBLIC_NETWORK_ERROR_MESSAGE = "网络连接失败，请检查网络后重试";
export const PUBLIC_UNAUTHORIZED_ERROR_MESSAGE = "登录已失效，请重新登录";
export const PUBLIC_AUTH_CLEAR_ERROR_MESSAGE = "登录状态清理失败，请刷新页面后重试";

function errorMessageFromValue(value) {
  if (typeof value === "string") {
    return value;
  }
  if (!value || typeof value !== "object") {
    return "";
  }

  if (typeof value.message === "string") {
    return value.message;
  }
  return errorMessageFromValue(value.error);
}

export function requestErrorMessage({ status, payload } = {}) {
  if (!Number.isInteger(status)) {
    return PUBLIC_NETWORK_ERROR_MESSAGE;
  }
  if (status >= 500) {
    return PUBLIC_SERVER_ERROR_MESSAGE;
  }

  const item = payload && typeof payload === "object" ? payload : null;
  return (
    errorMessageFromValue(item?.detail)
    || errorMessageFromValue(item?.error)
    || errorMessageFromValue(item?.message)
    || `请求失败 (${status})`
  );
}
