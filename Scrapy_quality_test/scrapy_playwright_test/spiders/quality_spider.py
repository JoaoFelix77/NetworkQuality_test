import scrapy
import time
import os
import json
import unicodedata
from bs4 import BeautifulSoup
import trafilatura
import chardet
from simhash import Simhash
from zss import simple_distance, Node
import lxml.etree as etree
from twisted.internet import threads
from scrapy_playwright.page import PageMethod

class QualitySpider(scrapy.Spider):
    name = "quality_spider"

    def __init__(self, urls=None, mode="http", output_dir="results", **kwargs):
        super().__init__(**kwargs)
        self.urls = urls or []
        self.mode = mode
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def start_requests(self):
        for url in self.urls:
            safe_url = self._safe_name(url)
            if self.mode == "http":
                meta = {
                    "start_time": time.time(),
                    "mode": "http",
                    "original_url": url,
                    "safe_url_name": safe_url,
                }
                yield scrapy.Request(
                    url, callback=self.parse, errback=self.errback,
                    meta=meta, dont_filter=True
                )
            else:
                pp_methods = [
                    PageMethod("wait_for_load_state", "networkidle")
                ]
                if self.mode == "chrome-no-media":
                    pp_methods.append(
                        PageMethod(
                            "route",
                            "**/*.{png,jpg,jpeg,gif,svg,mp4,webm}",
                            lambda route: route.abort()
                        )
                    )
                    pp_methods.append(
                        PageMethod(
                            "evaluate",
                            "() => document.querySelectorAll('img,video,source').forEach(e => e.remove());"
                        )
                    )
                # context 唯一化，确保每个请求单独生成 HAR 文件
                meta = {
                    "start_time": time.time(),
                    "mode": self.mode,
                    "original_url": url,
                    "safe_url_name": safe_url,
                    "playwright": True,
                    "playwright_context": f"{self.mode}_{safe_url}",
                    "playwright_include_page": True,
                    "playwright_page_kwargs": {"timeout": 30000},
                    "playwright_page_methods": pp_methods,
                }
                yield scrapy.Request(
                    url, callback=self.parse, errback=self.errback,
                    meta=meta, dont_filter=True
                )

    async def parse(self, response):
        url = response.meta.get("original_url", response.url)
        safe_url = response.meta.get("safe_url_name", self._safe_name(url))

        # 1. 获取原始 HTTP HTML
        raw_html = response.text
        with open(os.path.join(self.output_dir, f"{safe_url}_raw.html"), "w", encoding="utf-8") as f:
            f.write(raw_html)

        # 2. 如果有 Playwright page，则获取渲染后 DOM 与 JS 比
        page = response.meta.get("playwright_page")
        if page:
            try:
                rendered_html = await page.content()
            except Exception:
                rendered_html = raw_html

            try:
                js_ratio = await page.evaluate("""
                    () => {
                        const total = document.querySelectorAll('*').length;
                        const scripts = document.querySelectorAll('script').length;
                        return total === 0 ? 1.0 : scripts / total;
                    }
                """)
            except Exception:
                js_ratio = 1.0

            try:
                await page.close()
            except Exception:
                pass
        else:
            rendered_html = raw_html
            js_ratio = 1.0

        with open(os.path.join(self.output_dir, f"{safe_url}_rendered.html"), "w", encoding="utf-8") as f:
            f.write(rendered_html)

        # 静态质量指标（基于渲染后 HTML）
        soup = BeautifulSoup(rendered_html, "lxml")
        semantic_tags = len(soup.find_all(["p", "article", "section", "header", "main"]))
        main_text = trafilatura.extract(rendered_html) or ""
        main_ratio = round(len(main_text) / len(rendered_html), 3) if rendered_html else 0
        ad_tags = len(soup.find_all(["iframe", "ins"])) + len(soup.select("[class^='ad-']"))
        sim_value = Simhash(main_text).value if main_text else 0

        # 异步计算 DOM distance 并写入所有指标
        yield threads.deferToThread(
            self._compute_and_write,
            url,
            response.meta["mode"],
            raw_html,
            rendered_html,
            semantic_tags,
            main_ratio,
            ad_tags,
            sim_value,
            js_ratio,
            response.meta["start_time"]
        )

    def _compute_and_write(
        self, url, mode, raw_html, rendered_html,
        semantic_tags, main_ratio, ad_tags, sim_value,
        js_ratio, start_time
    ):
        # 计算 DOM 编辑距离
        if mode == "http":
            dom_distance = 0
        else:
            static_dom = etree.HTML(raw_html)
            dynamic_dom = etree.HTML(rendered_html)
            sn = self._element_to_node(static_dom)
            dn = self._element_to_node(dynamic_dom)
            if sn and dn:
                dom_distance = simple_distance(
                    sn, dn,
                    get_children=lambda x: x.children,
                    get_label=lambda x: x.label
                )
            else:
                dom_distance = abs(self._count_nodes(static_dom) - self._count_nodes(dynamic_dom))

        total_len = len(rendered_html)
        detected = chardet.detect(rendered_html.encode("utf-8", "ignore"))
        encoding = detected.get("encoding")
        encoding_error = int((encoding or "").lower() != "utf-8")
        garbage_chars = sum(1 for c in rendered_html if unicodedata.category(c).startswith("C"))
        garbage_rate = round(garbage_chars / total_len, 3) if total_len else 0
        load_time = round(time.time() - start_time, 3)

        item = {
            "url": url,
            "mode": mode,
            "total_length": total_len,
            "semantic_tags": semantic_tags,
            "main_ratio": main_ratio,
            "ad_tags": ad_tags,
            "simhash": sim_value,
            "dom_distance": dom_distance,
            "encoding_error": encoding_error,
            "garbage_rate": garbage_rate,
            "js_ratio": round(js_ratio, 3),
            "load_time": load_time,
        }
        metrics_file = os.path.join(self.output_dir, "quality_metrics.jsonl")
        with open(metrics_file, "a", encoding="utf-8") as mf:
            mf.write(json.dumps(item, ensure_ascii=False) + "\n")

    def _count_nodes(self, el):
        if el is None or not hasattr(el, "tag"):
            return 0
        return 1 + sum(self._count_nodes(c) for c in el)

    def _element_to_node(self, el):
        if el is None or not hasattr(el, "tag"):
            return None
        node = Node(el.tag)
        for c in el:
            child = self._element_to_node(c)
            if child:
                node.addkid(child)
        return node

    def errback(self, failure):
        self.logger.warning(f"[errback] {failure.request.url} failed: {failure.value}")

    def _safe_name(self, url):
        for ch in ["://", "/", "?", "=", "&", ";", "#", "%"]:
            url = url.replace(ch, "_")
        return url[:200]
