from __future__ import annotations

from typing import Any

from scrapers.noon_delivery_detection import DELIVERY_MARKER_JS_PATTERN


_CATEGORY_PRODUCT_CARDS_JS = r"""() => {
    const names = document.querySelectorAll('[data-qa="plp-product-box-name"]');
    const products = [];
    const collectSignalTexts = (card) => {
        const snippets = [];
        const seen = new Set();
        const push = (value) => {
            const normalized = (value || '').replace(/\\s+/g, ' ').trim();
            if (!normalized || normalized.length < 2 || normalized.length > 140) return;
            if (/^[\\d\\s.,%+-]+$/.test(normalized)) return;
            const key = normalized.toLowerCase();
            if (seen.has(key)) return;
            seen.add(key);
            snippets.push(normalized);
        };

        const nodes = card.querySelectorAll('span, div, p, small, strong, a, img, [aria-label], [title], [alt]');
        for (const node of nodes) {
            push(node.innerText || node.textContent || '');
            push(node.getAttribute && node.getAttribute('aria-label'));
            push(node.getAttribute && node.getAttribute('title'));
            push(node.getAttribute && node.getAttribute('alt'));
        }
        return snippets;
    };
    const isExplicitAdToken = (value) => {
        const normalized = (value || '').replace(/\\s+/g, ' ').trim();
        if (!normalized || normalized.length > 24) return false;
        const lowered = normalized.toLowerCase();
        return lowered === 'ad' || lowered === 'sponsored' || lowered === 'promoted' || normalized === '\\u0625\\u0639\\u0644\\u0627\\u0646';
    };
    const hasExplicitAdBadgeClass = (value) => {
        const normalized = (value || '').replace(/\\s+/g, ' ').trim();
        if (!normalized) return false;
        return /\\bSTag-module\\b/i.test(normalized);
    };
    const collectMarkerDetails = (card, pattern) => {
        const markers = [];
        const seen = new Set();
        const nodes = card.querySelectorAll('img, div, span, p, small, strong, a, button, [data-qa], [aria-label], [title], [alt]');

        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();

        for (const node of nodes) {
            const text = normalize(node.innerText || node.textContent || '');
            const dataQa = normalize(node.getAttribute && node.getAttribute('data-qa'));
            const alt = normalize(node.getAttribute && node.getAttribute('alt'));
            const title = normalize(node.getAttribute && node.getAttribute('title'));
            const ariaLabel = normalize(node.getAttribute && node.getAttribute('aria-label'));
            const className = normalize(
                typeof node.className === 'string'
                    ? node.className
                    : (node.className && node.className.baseVal) || ''
            );
            const attrBundle = normalize([dataQa, alt, title, ariaLabel, className].filter(Boolean).join(' | '));
            const hasExplicitAttrMatch = !!attrBundle && pattern.test(attrBundle);
            const hasShortTextMatch = !!text && text.length <= 80 && pattern.test(text);
            if (!hasExplicitAttrMatch && !hasShortTextMatch) continue;

            const merged = normalize([
                hasExplicitAttrMatch ? attrBundle : '',
                hasShortTextMatch ? text : '',
            ].filter(Boolean).join(' | '));
            if (!merged) continue;

            const dedupeKey = merged.toLowerCase();
            if (seen.has(dedupeKey)) continue;
            seen.add(dedupeKey);

            markers.push({
                tag: node.tagName || '',
                text: text,
                dataQa: dataQa,
                alt: alt,
                title: title,
                ariaLabel: ariaLabel,
                className: className,
                merged: merged,
            });
        }

        return markers;
    };
    const collectExplicitAdMarkers = (card) => {
        const markers = [];
        const seen = new Set();
        const nodes = card.querySelectorAll('img, div, span, p, small, strong, a, button, [data-qa], [aria-label], [title], [alt]');
        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();

        for (const node of nodes) {
            const text = normalize(node.innerText || node.textContent || '');
            const dataQa = normalize(node.getAttribute && node.getAttribute('data-qa'));
            const alt = normalize(node.getAttribute && node.getAttribute('alt'));
            const title = normalize(node.getAttribute && node.getAttribute('title'));
            const ariaLabel = normalize(node.getAttribute && node.getAttribute('aria-label'));
            const className = normalize(
                typeof node.className === 'string'
                    ? node.className
                    : (node.className && node.className.baseVal) || ''
            );
            const hasExplicitAdClassMatch = hasExplicitAdBadgeClass(className);

            const explicitMatches = [dataQa, alt, title, ariaLabel, text]
                .filter(Boolean)
                .filter(isExplicitAdToken);
            if (hasExplicitAdClassMatch) {
                explicitMatches.unshift('Ad');
            }
            if (!explicitMatches.length) continue;

            const merged = normalize([
                explicitMatches.join(' | '),
                hasExplicitAdClassMatch ? className : '',
            ].filter(Boolean).join(' | '));
            const dedupeKey = merged.toLowerCase();
            if (!merged || seen.has(dedupeKey)) continue;
            seen.add(dedupeKey);

            markers.push({
                tag: node.tagName || '',
                text: text,
                dataQa: dataQa,
                alt: alt,
                title: title,
                ariaLabel: ariaLabel,
                className: className,
                merged: merged,
            });
        }

        return markers;
    };

    for (let idx = 0; idx < names.length; idx++) {
        const nameEl = names[idx];
        const link = nameEl.closest('a');
        const href = link ? link.getAttribute('href') : '';
        const card = nameEl.closest('[class*="ProductBox"]') || nameEl.parentElement;
        const cardText = card ? card.innerText : '';

        const priceEl = card.querySelector('[data-qa="plp-product-box-price"]');
        const priceText = priceEl ? priceEl.innerText : '';

        const wasEl = card.querySelector('[class*="was" i], del, [class*="oldPrice" i], [class*="Was"]');
        const wasText = wasEl ? wasEl.innerText : '';

        const ratingEl = card.querySelector('[class*="Rating"], [class*="rating"]');
        const ratingText = ratingEl
            ? (
                ratingEl.getAttribute('aria-label')
                || ratingEl.getAttribute('title')
                || ratingEl.closest('[aria-label]')?.getAttribute('aria-label')
                || ratingEl.closest('[title]')?.getAttribute('title')
                || ratingEl.innerText
            )
            : '';

        const sellerEl = card.querySelector('[data-qa="plp-product-box-seller"], [class*="seller"], [class*="Seller"]');
        const sellerText = sellerEl ? sellerEl.innerText : '';

        const brandEl = card.querySelector('[data-qa="plp-product-brand"], [class*="brand"] a, [class*="Brand"] a');
        const brandText = brandEl ? brandEl.innerText : '';

        const imgs = card.querySelectorAll('img[src*="nooncdn.com/p/"], img[srcset*="nooncdn.com/p/"], img[data-src*="nooncdn.com/p/"]');
        const primaryImg = card.querySelector('img[src*="nooncdn.com/p/"], img[srcset*="nooncdn.com/p/"], img[data-src*="nooncdn.com/p/"]');
        const primaryImageUrl = primaryImg
            ? (
                primaryImg.currentSrc
                || primaryImg.getAttribute('src')
                || primaryImg.getAttribute('data-src')
                || primaryImg.getAttribute('data-original')
                || (((primaryImg.getAttribute('srcset') || '').split(',')[0] || '').trim().split(/\\s+/)[0])
            )
            : '';
        const signalTexts = collectSignalTexts(card);
        const deliveryMarkers = collectMarkerDetails(
            card,
            /__DELIVERY_MARKER_PATTERN__/i
        );
        const adMarkers = collectExplicitAdMarkers(card);

        const expressImg = card.querySelector('img[alt*="express" i]');
        const hasExpress = !!expressImg
            || deliveryMarkers.some(marker => /(product-noon-express|noon[-\\s]?express|\\bfbn\\b|\\bexpress\\b)/i.test(marker.merged || ''))
            || /express/i.test(cardText)
            || signalTexts.some(text => /express/i.test(text));

        const hasBestSeller = /best\\s*seller/i.test(cardText) || signalTexts.some(text => /best\\s*seller/i.test(text));

        const enhancedIsAd = adMarkers.length > 0 || signalTexts.some(text => isExplicitAdToken(text));

        products.push({
            title: nameEl.innerText.trim(),
            href: href,
            priceText: priceText,
            wasText: wasText,
            ratingText: ratingText,
            sellerText: sellerText,
            brandText: brandText,
            cardText: cardText,
            signalTexts: signalTexts,
            deliveryMarkers: deliveryMarkers,
            adMarkers: adMarkers,
            imgCount: imgs.length,
            imageUrl: primaryImageUrl,
            isExpress: hasExpress,
            isBestSeller: hasBestSeller,
            isAd: enhancedIsAd,
        });
    }
    return products;
}"""


def build_category_product_cards_js() -> str:
    return _CATEGORY_PRODUCT_CARDS_JS.replace("__DELIVERY_MARKER_PATTERN__", DELIVERY_MARKER_JS_PATTERN)


async def collect_category_product_payloads(page: Any) -> list[dict]:
    return await page.evaluate(build_category_product_cards_js())
