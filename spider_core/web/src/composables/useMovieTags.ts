/*
 * useMovieTags — 影片标签自适应折叠（还原旧版 movies.js fitMovieTags）
 * 在容器宽度内尽可能多地显示标签，溢出的折叠为 "+N" more 徽章。
 */
export function fitMovieTags(root: ParentNode = document) {
  root.querySelectorAll<HTMLElement>('.movie-tags').forEach((container) => {
    const tags = Array.from(container.querySelectorAll<HTMLElement>('[data-role="tag"]'))
    const more = container.querySelector<HTMLElement>('[data-role="more"]')
    if (!more) return
    tags.forEach((tag) => tag.classList.remove('hidden'))
    more.classList.add('hidden')
    more.style.display = 'none'
    more.innerText = '+0'
    const available = container.clientWidth
    if (!available) return
    let used = 0
    let visibleCount = 0
    const gap = 2
    for (const tag of tags) {
      const nextUsed = used + tag.offsetWidth + (visibleCount ? gap : 0)
      const remaining = tags.length - visibleCount - 1
      more.innerText = `+${remaining}`
      let moreWidth = 0
      if (remaining > 0) {
        more.classList.remove('hidden')
        more.style.display = ''
        more.classList.add('invisible')
        moreWidth = more.offsetWidth + gap
        more.classList.add('hidden')
        more.style.display = 'none'
        more.classList.remove('invisible')
      }
      if (nextUsed + moreWidth > available) break
      used = nextUsed
      visibleCount += 1
    }
    tags.forEach((tag, index) => tag.classList.toggle('hidden', index >= visibleCount))
    const hiddenCount = tags.length - visibleCount
    if (hiddenCount > 0) {
      more.innerText = `+${hiddenCount}`
      more.classList.remove('hidden')
      more.style.display = ''
    } else {
      more.innerText = '+0'
      more.classList.add('hidden')
      more.style.display = 'none'
    }
  })
}
