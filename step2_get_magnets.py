from curl_cffi import requests
from bs4 import BeautifulSoup
import re
import csv
import time
import os

# ================= 配置区域 =================
# 【重要】请将这里的信息配置得和第一步完全一样！
# 建议：运行前去 Edge 浏览器刷新一下页面，复制一个最新鲜的 Cookie 填进来，防止做到一半 Cookie 过期。
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0', # 替换为你的真实Edge UA
    'Cookie': '浏览器登录后获取cookie填写到这里',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Referer': 'https://javdb.com/'
}

PROXIES = {
    'http': 'http://192.168.2.1:7893',  # 替换为你的代理端口
    'https': 'http://192.168.2.1:7893'
}

# 输入和输出文件
INPUT_CSV = 'step1_list.csv'
OUTPUT_CSV = 'final_magnets.csv'
# ============================================

def parse_size(size_str):
    """解析文件大小为 MB"""
    if not size_str: return 0.0
    match = re.search(r'([\d\.]+)\s*(GB|MB|KB)', size_str.upper())
    if match:
        val = float(match.group(1))
        unit = match.group(2)
        if unit == 'GB': return val * 1024
        if unit == 'MB': return val
        if unit == 'KB': return val / 1024
    return 0.0

def evaluate_magnet(item_soup):
    """评估单个磁力链接的优先级"""
    # 提取磁力链接
    magnet_a = item_soup.select_one('a[href^="magnet:"]')
    if not magnet_a: return None
    magnet_link = magnet_a.get('href')
    
    # 提取文件名
    name_elem = item_soup.select_one('.name')
    name = name_elem.text.strip().lower() if name_elem else ''
    
    # 提取 Tags
    tags_elems = item_soup.select('.tags .tag')
    tags = [t.text.strip() for t in tags_elems]
    
    # 提取日期
    date_elem = item_soup.select_one('.date .time')
    date_str = date_elem.text.strip() if date_elem else '1970-01-01'
    
    # 提取大小
    meta_elem = item_soup.select_one('.meta')
    size_str = meta_elem.text.strip() if meta_elem else ''
    size_mb = parse_size(size_str)
    
    # === 优先级判断逻辑 ===
    has_sub = '-c' in name or 'chs' in name or '字幕' in tags
    has_uncensored = '-u' in name or 'uncensored' in name
    has_uc = '-uc' in name
    has_hd = '高清' in tags
    
    # 等级设定 (数字越大优先级越高)
    if has_uc or (has_sub and has_uncensored):
        rank = 5  # 无马赛克且有中文字幕
    elif has_sub:
        rank = 4  # 中文字幕
    elif has_uncensored:
        rank = 3  # 无马赛克
    elif has_hd:
        rank = 2  # 高清
    else:
        rank = 1  # 其他
        
    return {
        'link': magnet_link,
        'name': name_elem.text.strip() if name_elem else 'Unknown',
        'rank': rank,
        'date': date_str,
        'size_mb': size_mb
    }

def process_detail_pages():
    if not os.path.exists(INPUT_CSV):
        print(f"[!] 找不到输入文件 {INPUT_CSV}，请确认第一步已成功运行。")
        return

    # 读取第一步生成的清单
    movies = []
    with open(INPUT_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            movies.append(row)

    print(f"[*] 成功读取 {len(movies)} 部影片信息，准备开始爬取最优磁力链接...")

    # 准备写入最终结果
    fieldnames = ['影片番号', '原始标题', '影片链接', '最佳资源文件名', '磁力链接', '优先级得分', '日期', '文件大小(MB)']
    
    with open(OUTPUT_CSV, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, movie in enumerate(movies):
            url = movie['完整路径']
            print(f"\n[{i+1}/{len(movies)}] 正在解析番号: {movie['影片番号']}")
            
            try:
                # 携带配置请求详情页
                response = requests.get(
                    url, 
                    headers=HEADERS, 
                    proxies=PROXIES, 
                    impersonate="edge101", 
                    timeout=15
                )
                
                if response.status_code != 200:
                    print(f"  [!] 请求失败，状态码: {response.status_code}")
                    continue
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                magnets_content = soup.find(id='magnets-content')
                
                if not magnets_content:
                    print("  [-] 未找到磁力链接区块。")
                    continue
                    
                magnet_items = magnets_content.select('.item')
                valid_magnets = []
                
                for item in magnet_items:
                    mag_data = evaluate_magnet(item)
                    if mag_data:
                        valid_magnets.append(mag_data)
                        
                if not valid_magnets:
                    print("  [-] 区块内没有解析到有效的磁力链接。")
                    continue
                    
                # 【核心】：多条件排序
                # 规则：先按 rank 降序，再按 date 降序，最后按 size_mb 降序
                valid_magnets.sort(key=lambda x: (x['rank'], x['date'], x['size_mb']), reverse=True)
                best_mag = valid_magnets[0] # 取排在第一位的最优解
                
                writer.writerow({
                    '影片番号': movie['影片番号'],
                    '原始标题': movie['原始标题'],
                    '影片链接': url,
                    '最佳资源文件名': best_mag['name'],
                    '磁力链接': best_mag['link'],
                    '优先级得分': best_mag['rank'],
                    '日期': best_mag['date'],
                    '文件大小(MB)': round(best_mag['size_mb'], 2)
                })
                print(f"  [+] 匹配成功! (优先级等级: {best_mag['rank']}, 日期: {best_mag['date']}, 大小: {best_mag['size_mb']:.2f}MB)")
                print(f"  -> {best_mag['name']}")
                
            except Exception as e:
                print(f"  [!] 发生错误: {e}")
                
            # 延时防封禁（33部影片大概需要1分钟多一点的时间跑完）
            time.sleep(2)

    print(f"\n[*] 全部任务完成！最终数据已保存至: {OUTPUT_CSV}")

if __name__ == "__main__":
    process_detail_pages()