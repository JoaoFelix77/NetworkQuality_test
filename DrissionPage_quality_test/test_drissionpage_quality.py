import argparse
import time
import os
import json
import unicodedata
import chardet
import trafilatura
from bs4 import BeautifulSoup
from simhash import Simhash
from zss import simple_distance, Node
from lxml import etree
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from DrissionPage import ChromiumPage, ChromiumOptions

def load_urls(path='url_quality_list.txt'):
    with open(path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def element_to_node(element):
    if element is None or not hasattr(element, 'tag'):
        return None
    node = Node(element.tag)
    for child in element:
        child_node = element_to_node(child)
        if child_node:
            node.addkid(child_node)
    return node

def compute_metrics(html, body_bytes, load_time, mode, static_html=None):

    # 文本总长度
    total_len = len(html)

    # 语义化标签数
    soup = BeautifulSoup(html, 'lxml')
    semantic_tags = len(soup.find_all(['p', 'article', 'section', 'header', 'main']))
    
    # 正文占比
    main_text = trafilatura.extract(html) or ''
    main_ratio = round(len(main_text) / total_len, 3) if total_len else 0
    
    # 广告标签数
    ad_tags = len(soup.find_all(['iframe', 'ins'])) + len(soup.select('[class^="ad-"]'))
    
    # Simhash距离
    sim_value = Simhash(main_text).value if main_text else 0

    # 编码错误数
    detected = chardet.detect(body_bytes)
    declared = 'utf-8'
    encoding_error = int(detected.get('encoding', '').lower() != declared)
    
    # 乱码率
    garbage_chars = sum(1 for c in html if unicodedata.category(c).startswith('C'))
    garbage_rate = round(garbage_chars / total_len, 3) if total_len else 0
    
    # DOM编辑距离
    if static_html:
        static_dom = etree.HTML(static_html)
        dyn_dom = etree.HTML(html)
        static_node = element_to_node(static_dom)
        dyn_node = element_to_node(dyn_dom)
        dom_distance = simple_distance(static_node, dyn_node)
    else:
        dom_distance = 0

    # js依赖比
    if static_html:
        js_ratio = round(total_len / len(static_html), 3)
    else:
        js_ratio = 1.0
    
    return {
        'url': None,
        'mode': mode,
        'total_length': total_len,
        'semantic_tags': semantic_tags,
        'main_ratio': main_ratio,
        'ad_tags': ad_tags,
        'simhash': sim_value,
        'dom_distance': dom_distance,
        'encoding_error': encoding_error,
        'garbage_rate': garbage_rate,
        'js_ratio': js_ratio,
        'load_time': round(load_time, 3)
    }

def fetch_and_evaluate(url, mode, output_dir):
    static_html = None
    
    if mode != 'http':
        try:
            r = requests.get(url, timeout=20)
            static_html = r.text
        except:
            static_html = None

    start = time.time()
    if mode == 'http':
        resp = requests.get(url, timeout=20)
        html = resp.text
        body_bytes = resp.content
    else:
        co = ChromiumOptions()
        if mode == 'chrome-no-js':
            co.set_argument('--disable-javascript')
        elif mode == 'chrome-no-media':
            co.set_argument('--blink-settings=imagesEnabled=false')
            co.set_argument('--disable-plugins')
        page = ChromiumPage(co)
        page.get(url, timeout=30)
        html = page.html
        body_bytes = html.encode('utf-8')
        page.close()
    load_time = time.time() - start
    metrics = compute_metrics(html, body_bytes, load_time, mode, static_html)
    metrics['url'] = url
   
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, 'quality_metrics.jsonl')
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(metrics, ensure_ascii=False) + '\n')


def main():
    parser = argparse.ArgumentParser(description='DrissionPage 网页质量评估')
    parser.add_argument('--mode', required=True,
                        choices=['http', 'chrome', 'chrome-no-js', 'chrome-no-media'],
                        help='抓取模式')
    parser.add_argument('--concurrency', type=int, default=4, help='并发数')
    parser.add_argument('--output_dir', default='results', help='输出目录')
    args = parser.parse_args()

    urls = load_urls()
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        for url in urls:
            executor.submit(fetch_and_evaluate, url, args.mode, args.output_dir)

if __name__ == '__main__':
    main()
    print(f"results saved to quality_metrics.jsonl")
