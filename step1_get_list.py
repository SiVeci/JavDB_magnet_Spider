from curl_cffi import requests
from bs4 import BeautifulSoup
import urllib.parse
import re
import csv
import time

# ================= 配置区域 =================
# 【极度重要】这里填写的 User-Agent 必须是你浏览器里抓到的那个！
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0', # 请替换为你真实的 Edge UA
    'Cookie': '浏览器登录后获取cookie填写到这里',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Referer': 'https://javdb.com/'
}

START_URL = 'https://javdb.com/actors/GMN7?t=s,d&sort_type=0'
OUTPUT_CSV = 'step1_list.csv'

# 如果你的 Edge 浏览器能直接访问（开了系统代理），这里也必须配置相同的代理
PROXIES = {
    'http': 'http://192.168.2.1:7893',  # 修改为你的代理软件端口
    'https': 'http://192.168.2.1:7893'
}
# ============================================

def fetch_movie_list(start_url):
    print(f"[*] 开始网络请求，拉取影片列表，起始页：{start_url}")
    results = []
    current_url = start_url
    page_count = 1
    
    while current_url:
        print(f"  -> 正在抓取第 {page_count} 页: {current_url}")
        try:
            #impersonate="edge101" 模拟 Edge 浏览器的 TLS 指纹
            response = requests.get(
                current_url, 
                headers=HEADERS, 
                proxies=PROXIES, 
                impersonate="edge101", 
                timeout=15
            )
            
            if response.status_code != 200:
                print(f"[!] 网页返回状态码异常: {response.status_code}")
                if response.status_code == 403:
                    print("[!] 依旧返回 403。请确保 Cookie 是刚刚从 Edge 复制的，且代理端口正确。")
                break
                
            soup = BeautifulSoup(response.text, 'html.parser')
            movie_items = soup.select('div.movie-list a.box')
            
            if not movie_items:
                print("[!] 抓取成功，但没找到影片，可能是 Cookie 失效导致未登录。")
                break

            for item in movie_items:
                path = item.get('href')
                full_url = urllib.parse.urljoin('https://javdb.com', path)
                raw_title = item.get('title', '')
                code_match = re.search(r'^[A-Za-z0-9\-]+', raw_title)
                code = code_match.group(0) if code_match else "未知番号"
                
                if not any(d['完整路径'] == full_url for d in results):
                    results.append({
                        '影片番号': code,
                        '完整路径': full_url,
                        '原始标题': raw_title
                    })
            
            next_page_btn = soup.select_one('nav.pagination a.pagination-next')
            if next_page_btn and next_page_btn.get('href'):
                current_url = urllib.parse.urljoin('https://javdb.com', next_page_btn.get('href'))
                page_count += 1
                time.sleep(2) 
            else:
                print("  -> 没有下一页了，列表抓取结束。")
                break
                
        except Exception as e:
            print(f"[!] 请求 {current_url} 时发生错误: {e}")
            break
            
    return results

def save_to_csv(data, filename):
    if not data:
        return
    keys = data[0].keys()
    with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    print(f"[*] 成功！共抓取 {len(data)} 部影片链接，已保存至: {filename}")

if __name__ == "__main__":
    movie_list = fetch_movie_list(START_URL)
    save_to_csv(movie_list, OUTPUT_CSV)