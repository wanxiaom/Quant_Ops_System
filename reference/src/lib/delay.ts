export function delay(ms = 260) {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms)
  })
}
