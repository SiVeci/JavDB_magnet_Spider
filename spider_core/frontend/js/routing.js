// Database routing helpers.

const DATABASE_TYPE_ACTOR = 'actor';

const DATABASE_TYPE_RANKING = 'ranking';

function databaseTypeLabel(type) {
    if (type === DATABASE_TYPE_ACTOR) return '演员';
    if (type === DATABASE_TYPE_RANKING) return '排行榜';
    return '';
}

function isDatabaseType(type) {
    return [DATABASE_TYPE_ACTOR, DATABASE_TYPE_RANKING].includes(type);
}

function databaseRouteParts() {
    const hash = window.location.hash || '#/database';
    const path = hash.replace(/^#\/?/, '');
    if (!path.startsWith('database')) return [];
    return path.split('/').slice(1).map(part => decodeURIComponent(part));
}

function databaseRouteInfo() {
    const parts = databaseRouteParts();
    if (!parts.length) {
        return { type: null, collectionName: null, movieId: null, legacy: false };
    }
    const [first, second, third, fourth] = parts;
    if (isDatabaseType(first)) {
        if (first === DATABASE_TYPE_ACTOR) {
            return { type: first, collectionName: second || null, movieId: third || null, legacy: false };
        }
        return { type: first, category: second || null, period: third || null, collectionName: null, movieId: fourth || null, legacy: false };
    }
    return { type: DATABASE_TYPE_ACTOR, collectionName: first, movieId: second || null, legacy: true };
}

function currentDatabaseMovieId() {
    return databaseRouteInfo().movieId;
}

function databaseHash() {
    return '#/database';
}

function databaseActorHash(collectionName = null, movieId = null) {
    let hash = `#/database/${DATABASE_TYPE_ACTOR}`;
    if (collectionName) hash += `/${encodeURIComponent(collectionName)}`;
    if (movieId) hash += `/${encodeURIComponent(String(movieId))}`;
    return hash;
}

function databaseTypeHash(type) {
    return `#/database/${encodeURIComponent(type)}`;
}

function databaseRankingHash(category = null, period = null, movieId = null) {
    let hash = `#/database/${DATABASE_TYPE_RANKING}`;
    if (category) hash += `/${encodeURIComponent(category)}`;
    if (period) hash += `/${encodeURIComponent(period)}`;
    if (movieId) hash += `/${encodeURIComponent(String(movieId))}`;
    return hash;
}

function setDatabaseHash(collectionName = null, movieId = null) {
    const hash = collectionName ? databaseActorHash(collectionName, movieId) : databaseHash();
    if (window.location.hash === hash) {
        renderDatabaseRoute();
    } else {
        window.location.hash = hash;
    }
}

function setDatabaseTypeHash(type) {
    const hash = databaseTypeHash(type);
    if (window.location.hash === hash) {
        renderDatabaseRoute();
    } else {
        window.location.hash = hash;
    }
}

function setRankingHash(category = null, period = null, movieId = null) {
    const hash = databaseRankingHash(category, period, movieId);
    if (window.location.hash === hash) {
        renderDatabaseRoute();
    } else {
        window.location.hash = hash;
    }
}

function renderDatabaseBreadcrumb(collectionName = null, movie = null, options = {}) {
    const box = databaseBreadcrumb();
    if (!box) return;
    const type = options.type || (collectionName ? DATABASE_TYPE_ACTOR : null);
    const items = [
        `<button type="button" onclick="setDatabaseHash()" class="font-bold text-[color:var(--c-primary-text)] hover:underline">数据库</button>`
    ];
    if (type) {
        items.push(`<button type="button" onclick="setDatabaseTypeHash('${escapeJs(type)}')" class="font-bold text-[color:var(--c-primary-text)] hover:underline">${databaseTypeLabel(type)}</button>`);
    }
    if (collectionName) {
        items.push(`<button type="button" onclick="setDatabaseHash('${escapeJs(collectionName)}')" class="max-w-[42vw] truncate font-bold text-[color:var(--c-primary-text)] hover:underline">${escapeHtml(displayName(collectionName))}</button>`);
    }
    if (options.rankingCategory) {
        const category = options.rankingCategory;
        items.push(`<button type="button" onclick="setRankingHash('${escapeJs(category.key)}')" class="font-bold text-[color:var(--c-primary-text)] hover:underline">${escapeHtml(category.label)}</button>`);
    }
    if (options.rankingPeriod) {
        const period = options.rankingPeriod;
        if (options.rankingCategory) {
            const category = options.rankingCategory;
            items.push(`<button type="button" onclick="setRankingHash('${escapeJs(category.key)}', '${escapeJs(period.key)}')" class="font-bold text-[color:var(--c-primary-text)] hover:underline">${escapeHtml(period.label)}</button>`);
        } else {
            items.push(`<span class="font-bold text-slate-700">${escapeHtml(period.label)}</span>`);
        }
    }
    if (movie) {
        items.push(`<span class="max-w-[42vw] truncate font-bold text-slate-700">${escapeHtml(movie.code || String(movie.id))}</span>`);
    }
    box.innerHTML = `<div class="flex min-w-0 flex-wrap items-center gap-2">${items.join('<span class="text-slate-300">/</span>')}</div>`;
}

async function renderDatabaseRoute() {
    renderGlobalMagnetCheckButton();
    updateDatabaseSummary();
    const route = databaseRouteInfo();
    if (!route.type) {
        renderDatabaseTypePage();
        return;
    }
    if (route.type === DATABASE_TYPE_RANKING) {
        if (!route.category) {
            renderRankingCategoryPage();
            return;
        }
        const category = rankingCategoryMeta(route.category);
        if (!category) {
            setDatabaseTypeHash(DATABASE_TYPE_RANKING);
            showToast('排行榜分类不存在');
            return;
        }
        if (!route.period) {
            if (category.dynamicOptions) {
                await renderTop250OptionPage(category);
                return;
            }
            renderRankingPeriodPage(category);
            return;
        }
        if (category.dynamicOptions && !top250OptionCache) {
            try {
                await loadTop250Options(false);
            } catch (err) {
                console.warn(err);
            }
        }
        const period = rankingPeriodMetaForCategory(category, route.period);
        if (!period) {
            setRankingHash(category.key);
            showToast('榜单周期不存在');
            return;
        }
        if (route.movieId) {
            await renderRankingMagnetListPage(category, period, route.movieId);
        } else {
            await renderRankingMovieListPage(category, period);
        }
        return;
    }
    if (!route.collectionName) {
        renderCollectionListPage();
        return;
    }
    setCollectionToolbarVisible(false);
    if (!collectionsCache.length) {
        showDatabaseLoading();
        return;
    }
    if (!collectionItem(route.collectionName)) {
        setDatabaseHash();
        showToast('数据集合不存在或已被删除');
        return;
    }
    if (route.legacy) {
        setDatabaseHash(route.collectionName, route.movieId);
        return;
    }
    showDatabaseLoading();
    if (!(await ensureCollectionMovies(route.collectionName))) return;
    if (route.movieId) {
        await renderMagnetListPage(route.collectionName, route.movieId);
    } else {
        renderMovieListPage(route.collectionName);
    }
}
