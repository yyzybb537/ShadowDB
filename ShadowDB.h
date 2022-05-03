#pragma once

#include <map>
#include <set>
#include <memory>
#include <string>
#include <vector>
#include <list>
#include <iostream>
#include <climits>
#include <queue>
#include <algorithm>
#include <unordered_map>
#include <string.h>
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>

namespace shadow {

using namespace std;

namespace adl {
    template <typename T>
    struct HasToString
    {
        template <typename U>
        static char foo(U*, decltype(((U*)0)->toString())*);

        template <typename U>
        static short foo(U*, ...);

        static const bool value = sizeof(foo((T*)0, nullptr)) == sizeof(char);
    };

    template <typename T>
    struct StdToString
    {
        template <typename U>
        static char foo(U*, decltype(std::to_string(*((U*)0)))*);

        template <typename U>
        static short foo(U*, ...);

        static const bool value = sizeof(foo((T*)0, nullptr)) == sizeof(char);
    };

    template <typename T>
    struct IsString
    {
        static const bool value = std::is_same<typename std::remove_cv<T>::type, std::string>::value
            || std::is_same<typename std::remove_cv<T>::type, char*>::value
            || std::is_same<typename std::remove_cv<T>::type, const char*>::value;
    };

    template <typename T>
    static typename std::enable_if<StdToString<T>::value, string>::type
    to_string(T const& t)
    {
        return std::to_string(t);
    }

    template <typename T>
    static typename std::enable_if<HasToString<T>::value, string>::type
    to_string(T const& t)
    {
        return const_cast<T&>(t).toString();
    }

    template <typename T>
    static typename std::enable_if<IsString<T>::value, string>::type
    to_string(T const& t)
    {
        return string{t};
    }

    template <typename T>
    static typename std::enable_if<
        !HasToString<T>::value && !StdToString<T>::value && !IsString<T>::value,
        string
    >::type
    to_string(T const& t)
    {
        return "Unsupport-type";
    }

    template <typename V>
    map<size_t, string> & field_maps()
    {
        static map<size_t, string> m;
        return m;
    }

    template <typename V, typename F>
    void register_to_string(F V::* memptr, string fieldname)
    {
        size_t offset = reinterpret_cast<size_t>(&(((V*)0) ->* memptr));
        map<size_t, string> & m = field_maps<V>();
        m[offset] = fieldname;
    }

#define SHADOW_DB_DEBUG_FIELD(V, F) \
    struct SHADOW_DB_DEBUG_FIELD__ ## V ## F                        \
    {                                                               \
        SHADOW_DB_DEBUG_FIELD__ ## V ## F () {                      \
            ::shadow::adl::register_to_string(&V::F, #V "::" #F);   \
        }                                                           \
    } gShadowDBDebugRegister_ ## V ## F

    template <typename V, typename F>
    string field_to_string(F V::* memptr)
    {
        size_t offset = reinterpret_cast<size_t>(&(((V*)0) ->* memptr));
        map<size_t, string> & m = field_maps<V>();
        auto it = m.find(offset);
        return (m.end() == it) ? "" : it->second;
    }
}

string P(const char* fmt = "", ...)  __attribute__((format(printf,1,2)));
string fmt(const char* fmt = "", ...)  __attribute__((format(printf,1,2)));

size_t & tlsTab() {
    static thread_local size_t t = 0;
    return t;
}

string P(const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    char buf[4096];
    size_t tt = tlsTab();
    if (tt) {
        memset(buf, ' ', tt);
    }

    int len = vsnprintf((char*)buf + tt, sizeof(buf) - tt - 1, fmt, ap) + tt;
    buf[len] = '\n';
    va_end(ap);
    return std::string(buf, len + 1);
}

string fmt(const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    char buf[4096];
    int len = vsnprintf((char*)buf, sizeof(buf) - 1, fmt, ap);
    va_end(ap);
    return std::string(buf, len);
}

// 条件
enum class e_cond_op : char
{
    none = 0,
    lt = 1, // <
    le = 2, // <=
    eq = 3, // ==
    ge = 4, // >=
    gt = 5, // >
    ne = 6, // !=
};

const char* e_cond_op_2_str(e_cond_op op)
{
    switch ((int)op)
    {
        case (int)e_cond_op::lt:
            return "<";

        case (int)e_cond_op::le:
            return "<=";

        case (int)e_cond_op::eq:
            return "==";

        case (int)e_cond_op::ge:
            return ">=";

        case (int)e_cond_op::gt:
            return ">";

        case (int)e_cond_op::ne:
            return "!=";
    }
    return "";
}

struct Debugger
{
    struct IndexHintInfo
    {
        string indexName;
        size_t nLeftMatched = 0;    // 最左匹配
        size_t nMatchedCond = 0;    // 匹配到的条件数量
        size_t nForkLevels = 0;     // 数据层数
        size_t nScanIndexKeys = 0;  // 遍历过的索引key
        size_t nScanRows = 0;       // 遍历过的数据行
        size_t nResultRows = 0;     // 返回的结果行数

        string toString(bool matched = false)
        {
            string s;
            s += P("[%s]", indexName.c_str());
            ++tlsTab();
            s += P("left-matched: %d", (int)nLeftMatched);
            s += P("matched-cond: %d", (int)nMatchedCond);
            s += P("fork-levels:  %d", (int)nForkLevels);
            if (matched) {
                s += P("scan-index-k: %d", (int)nScanIndexKeys);
                s += P("scan-rows:    %d", (int)nScanRows);
                s += P("result-rows:  %d", (int)nResultRows);
            }
            --tlsTab();
            return s;
        }
    };

    // 追踪单次索引使用
    struct OnceIndexQueryTrace
    {
        string cond;
        std::vector<IndexHintInfo> tryMatchIndexes;
        IndexHintInfo matched;

        string toString()
        {
            string s;
            s += P("cond: %s", cond.c_str());
            s += P("tryMatchIndexes:");
            ++tlsTab();
            for (size_t i = 0; i < tryMatchIndexes.size(); ++i) {
                s += P("[%d]", (int)i);
                ++tlsTab();
                s += tryMatchIndexes[i].toString(false);
                --tlsTab();
            }
            --tlsTab();
            s += P("matched-index:");
            ++tlsTab();
            s += matched.toString(true);
            --tlsTab();
            return s;
        }
    };

    struct QueryTrace
    {
        string or_cond;
        string optimizedCond;
        std::list<OnceIndexQueryTrace> querys;

        // 遍历过的索引key
        size_t getScanIndexKeys() {
            size_t n = 0;
            for (auto & q : querys)
                n += q.matched.nScanIndexKeys;
            return n;
        }

        // 遍历过的数据行
        size_t getScanRows() {
            size_t n = 0;
            for (auto & q : querys)
                n += q.matched.nScanRows;
            return n;
        }


        // 返回的结果行数
        size_t getResultRows() {
            size_t n = 0;
            for (auto & q : querys)
                n += q.matched.nResultRows;
            return n;
        }

        OnceIndexQueryTrace* newQuery()
        {
            querys.push_back(OnceIndexQueryTrace{});
            return &querys.back();
        }

        string toString()
        {
            string s;
            s += P("cond:           %s", or_cond.c_str());
            s += P("optimized-cond: %s", optimizedCond.c_str());
            s += P("scan-index-k:   %d", (int)getScanIndexKeys());
            s += P("scan-rows:      %d", (int)getScanRows());
            s += P("result-rows:    %d", (int)getResultRows());
            s += P("querys (size=%d):", (int)querys.size());
            ++tlsTab();
            int i = 0;
            for (auto & q : querys) {
                s += P("[%d]", (int)i++);
                ++tlsTab();
                s += q.toString();
                --tlsTab();
            }
            --tlsTab();
            return s;
        }
    };

    QueryTrace queryTrace;

    string toString()
    {
        string s;
        s += P("[Debugger]");
        ++tlsTab();
        s += P("query-trace:");
        ++tlsTab();
        s += queryTrace.toString();
        --tlsTab();
        --tlsTab();
        return s;
    }
};

// 多层结构
// 最上面一层当做logs使用, 可修改, 底层不可变更
template <
    typename K,
    typename V,
    template <typename K, typename V> class Table
>
struct ShadowBase
{
public:
    struct VStorage : public V
    {
        VStorage() = default;
        VStorage(VStorage const&) = default;
        VStorage(VStorage &&) = default;
        VStorage& operator=(VStorage const&) = default;
        VStorage& operator=(VStorage &&) = default;

        VStorage(V const& v) : V(v) {}
        VStorage(V && v) : V(std::move(v)) {}
        VStorage& operator=(V const& v) {
            static_cast<V&>(*this) = v;
            return *this;
        }
        VStorage& operator=(V && v) {
            static_cast<V&>(*this) = std::move(v);
            return *this;
        }

        string toString()
        {
            string s;
            s = fmt("%s{%s}", deleted ? "(D)" : "",
                    adl::to_string(static_cast<V&>(*this)).c_str());
            return s;
        }

        bool deleted = false;
    };

    typedef Table<K, VStorage> table_t;
    typedef shared_ptr<table_t> table_ptr;

    ShadowBase() = default;
    ShadowBase(ShadowBase const&) = delete;
    ShadowBase& operator=(ShadowBase const&) = delete;

    virtual table_ptr makeTable() = 0;
    virtual void forUniqueTable(table_t & table) {}       // 初始化成单层table(不清理数据)
    virtual void forBaseTable(table_t & table) {}         // 初始化成底层table, 上层还有logs(不清理数据)
    virtual void forLogsTable(table_t & table) {}         // 初始化成顶层table(不清理数据)
    virtual void mergeTable(table_t & from, table_t & to) = 0;   // 覆盖合并
    virtual string tableToString(table_t & table, bool simple) = 0;

    void reset() {
        levels_.clear();
        levels_.push_back(makeTable());
        forUniqueTable(*logs());
    }

    void fork(ShadowBase & other)
    {
        if (logs()->empty() && level() == 1) {
            // 空的
            other.reset();
            return ;
        }

        other.levels_.clear();

        if (logs()->empty()) {
            // 没有logs, 顶层不需要共享
            for (size_t i = 0; i + 1 < level(); ++i) {
                other.levels_.push_back(levels_[i]);
            }
        } else {
            // 所有层共享
            for (auto & ht : levels_) {
                other.levels_.push_back(ht);
            }

            addLevel();
        }

        other.addLevel();
    }

    void merge()
    {
        size_t mergeStartLevel = 0;
        table_ptr htp;
        table_ptr & base = levels_.front();
        if (base.unique()) {
            // 没有别的共享了, 可以直接merge到最底层
            mergeStartLevel = 1;
            htp = base;
        } else {
            // 最底层有共享的, 需要创建一个新的hashtable
            htp = makeTable();
        }

        forUniqueTable(*htp);

        // 逐层合并
        for (size_t i = mergeStartLevel; i < levels_.size(); ++i)
        {
            table_t & ht = *levels_[i];
            mergeTable(ht, *htp);
        }

        levels_.clear();
        levels_.push_back(htp);
    }

    size_t level() {
        return levels_.size();
    }

    string toString(bool simple = false)
    {
        string s;
        s += P("[Shadow-Table] (level=%d)", (int)level());
        ++tlsTab();
        for (size_t i = 0; i < level(); ++i) {
            table_ptr & tp = table(i);
            s += P("[%d](ref=%d)(0x%p) %s",
                    (int)i, (int)tp.use_count(), (void*)tp.get(),
                    (i == 0) ? "-> top" : "");
            ++tlsTab();
            s += tableToString(*tp, simple);
            --tlsTab();
        }
        --tlsTab();
        return s;
    }

protected:
    inline table_ptr & logs()
    {
        return levels_.back();
    }

    inline table_ptr & table(size_t lv)
    {
        assert(lv < levels_.size());
        return levels_[levels_.size() - lv - 1];
    }

    void addLevel()
    {
        // 冻结上一层的rehash
        if (level() == 1) {
            forBaseTable(*logs());
        }

        // 新增一层, bucket_count和上一层保持一致, 并冻结rehash
        table_ptr ht = makeTable();
        forLogsTable(*ht);
        levels_.push_back(ht);
    }

protected:
    vector<table_ptr> levels_;
};

template <typename K, typename V>
using hashtable = std::unordered_map<K, V>;

template <typename K, typename V>
struct ShadowHashTable : public ShadowBase<K, V, hashtable>
{
public:
    typedef ShadowHashTable<K, V> this_t;
    typedef ShadowBase<K, V, hashtable> base_t;
    typedef typename base_t::VStorage VStorage;
    typedef typename base_t::table_t table_t;
    typedef typename base_t::table_ptr table_ptr;

    using base_t::reset;
    using base_t::logs;
    using base_t::level;
    using base_t::table;

    struct ref_t
    {
        K key;
        size_t bucket_count = 0;
        size_t bucket_index = 0;

        ref_t() = default;
        explicit ref_t(K const& k) : key(k) {}

        friend bool operator<(ref_t const& lhs, ref_t const& rhs) {
            return lhs.key < rhs.key;
        }

        friend bool operator==(ref_t const& lhs, ref_t const& rhs) {
            return lhs.key == rhs.key;
        }

        string toString()
        {
            string s;
            s += fmt("{key=%s, bc=%d, bi=%d}",
                    adl::to_string(key).c_str(),
                    (int)bucket_count, (int)bucket_index);
            return s;
        }
    };

    struct iterator
    {
    public:
        iterator() = default;
        explicit iterator(this_t * hashtable) : ht(hashtable) {}

        iterator(iterator &&) = default;
        iterator& operator=(iterator &&) = default;

        iterator(iterator const&) = delete;
        iterator& operator=(iterator const&) = delete;

        bool isEnd() const { return bEnd; }
        explicit operator bool() const { return !isEnd(); }

        typename table_t::value_type & operator*() { return *it; }
        typename table_t::value_type const& operator*() const { return *it; }
        typename table_t::value_type * operator->() { return &*it; }
        typename table_t::value_type const* operator->() const { return &*it; }

        ref_t & getRef() { return ref; }
        ref_t const& getRef() const { return ref; }

        iterator & operator++()
        {
            next();
            return *this;
        }

        void setBegin()
        {
            bucket = 0;
            level = 0;
            bucketUnique.clear();
            bEnd = false;
            it = ht->table(level)->begin(bucket);
            if (!begin()) {
                setEnd();
            }
        }

        void setEnd() { bEnd = true; }

        friend bool operator==(iterator const& lhs, iterator const& rhs)
        {
            if (lhs.isEnd() && rhs.isEnd()) return true;

            if (lhs.ht != rhs.ht) return false;
            return lhs.bucket == rhs.bucket && lhs.level == rhs.level
                && lhs.it == rhs.it;
        }

        friend bool operator!=(iterator const& lhs, iterator const& rhs)
        {
            return !(lhs == rhs);
        }

    private:
        void next()
        {
            ++it;
            if (!begin()) {
                setEnd();
            }
        }

        bool begin()
        {
            while (bucket < ht->logs()->bucket_count()) {
                while (level < ht->level()) {
                    if (ht->table(level)->end(bucket) == it) {
                        ++level;
                        if (level == ht->level())
                            break;

                        it = ht->table(level)->begin(bucket);
                        continue;
                    }

                    if (!bucketUnique.insert(it->first).second) {
                        ++it;
                        continue;
                    }

                    if (it->second.deleted) {
                        ++it;
                        continue;
                    }

                    ref.key = it->first;
                    ref.bucket_count = ht->logs()->bucket_count();
                    ref.bucket_index = bucket;
                    return true;
                }

                level = 0;
                bucketUnique.clear();
                ++bucket;
                if (bucket == ht->logs()->bucket_count())
                    break;

                it = ht->table(level)->begin(bucket);
            }

            return false;
        }

    private:
        this_t * ht;
        size_t bucket = 0;
        size_t level = 0;
        typename table_t::local_iterator it;
        std::set<K> bucketUnique;
        bool bEnd = true;
        ref_t ref;
    };

    bool foreach(std::function<bool(K const&, ref_t const&, V*)> pred)
    {
        iterator it(this);
        it.setBegin();
        for (; it; ++it) {
            if (!pred(it->first, it.getRef(), static_cast<V*>(&it->second)))
                return false;
        }

        return true;
    }

    explicit ShadowHashTable(size_t minBucketCount = 1024)
        : minBucketCount_(minBucketCount)
    {
        reset();
    }

    iterator begin()
    {
        iterator it(this);
        it.setBegin();
        return std::move(it);
    }

    void fork(ShadowHashTable & other)
    {
        other.minBucketCount_ = minBucketCount_;
        base_t::fork(other);
    }

    void set(K const& key, V const& value)
    {
        VStorage & vs = (*logs())[key];
        static_cast<V&>(vs) = value;
        vs.deleted = false;
    }

    void del(K const& key)
    {
        if (level() == 1) {
            logs()->erase(key);
            return ;
        }

        // 查看logs以下的层中是否有key
        ref_t keyRef(key);
        ref_t out;
        for (size_t i = 1; i < level(); ++i) {
            table_ptr & htp = table(i);
            std::pair<V*, bool> res = get(htp, keyRef, out);
            if (res.first) {
                // 底层有, logs追加一个deleted记录
                VStorage & vs = (*logs())[key];
                vs.deleted = true;
                return ;
            }

            if (res.second) {   // 顶层已删除, 底层无需再看
                break;
            }
        }

        // 底层没有or已删除, logs里直接删除即可
        logs()->erase(key);
    }

    V* get(K const& key)
    {
        ref_t out;
        return get(key, out);
    }

    V* get(K const& key, ref_t & out)
    {
        ref_t keyRef;
        keyRef.key = key;
        return get(keyRef, out);
    }

    V* get(ref_t const& keyRef, ref_t & out)
    {
        // 自顶向下, 逐层查找
        for (size_t i = 0; i < level(); ++i) {
            table_ptr & htp = table(i);
            std::pair<V*, bool> res = get(htp, keyRef, out);
            if (res.first) {
                return res.first;
            }

            if (res.second) {   // 顶层已删除, 底层无需再看
                return nullptr;
            }
        }

        return nullptr;
    }

    static float& default_load_factor()
    {
        static float load_factor = 0.5;
        return load_factor;
    }

protected:
    virtual table_ptr makeTable() override
    {
        return std::make_shared<table_t>();
    }

    virtual void forUniqueTable(table_t & table) override
    {
        // 打开rehash
        table.max_load_factor(default_load_factor());
        table.reserve(minBucketCount_ * default_load_factor());
    }

    virtual void forBaseTable(table_t & table) override
    {
        // 冻结rehash
        table.max_load_factor(std::numeric_limits<float>::max());
    }

    virtual void forLogsTable(table_t & table) override
    {
        // 和上一层保持相同bucket_count & 冻结rehash
        table.max_load_factor(1);
        table.reserve(logs()->bucket_count());
        table.max_load_factor(std::numeric_limits<float>::max());
    }

    virtual void mergeTable(table_t & from, table_t & to) override
    {
        for (auto const& kv : from)
        {
            if (kv.second.deleted) {
                to.erase(kv.first);
            } else {
                to[kv.first] = kv.second;
            }
        }
    }

    virtual string tableToString(table_t & table, bool simple) override
    {
        string s;
        s += P("Hashtable[size=%d bucket=%d load_factor=%.3f max_load_factor=%.3f]",
                (int)table.size(), (int)table.bucket_count(),
                (float)table.load_factor(), (float)table.max_load_factor());
        if (simple)
            return s;

        ++tlsTab();
        for (size_t i = 0; i < table.bucket_count(); ++i)
        {
            for (auto it = table.begin(i); it != table.end(i); ++it)
            {
                s += P("[%d] K=%s V=%s", (int)i,
                        adl::to_string(it->first).c_str(),
                        adl::to_string(it->second).c_str());
            }
        }
        --tlsTab();
        return s;
    }

    // @return: <pointer, isDeleted>
    std::pair<V*, bool> get(table_ptr & htp, ref_t const& keyRef, ref_t & out)
    {
        size_t bucket_index = keyRef.bucket_index;
        if (htp->bucket_count() != keyRef.bucket_count) {
            // bucket index失效, 重新hash
            bucket_index = htp->bucket(keyRef.key);
        }

        for (auto it = htp->begin(bucket_index);
                it != htp->end(bucket_index); ++it)
        {
            if (it->first != keyRef.key)
                continue;

            VStorage & vs = it->second;
            if (vs.deleted) {
                return {nullptr, true};
            }

            out.key = it->first;
            out.bucket_count = htp->bucket_count();
            out.bucket_index = bucket_index;
            return {static_cast<V*>(&vs), false};
        }

        return {nullptr, false};
    }

private:
    size_t minBucketCount_;
};

// 备选结构：rbtree
// 优点: 遍历有序, fork快
// 缺点: 查询慢, fork多层以后更慢
//template <typename K, typename V>
//using rbtree = std::map<K, V>;
//
//template <typename K, typename V>
//struct ShadowRBTree : public ShadowBase<K, V, rbtree>;

// 递归map-set结构
// map->map->map->...->set
template <typename K, typename V>
struct RecursiveMapSet
{
public:
    typedef RecursiveMapSet<K, V> this_t;
    typedef std::set<V> set_t;
    typedef typename set_t::iterator set_iterator;
    typedef void* map_value_type;
    typedef std::map<K, void*> map_t;
    typedef typename map_t::iterator map_iterator;
    typedef std::vector<K> keys_t;
    typedef std::pair<K, e_cond_op> cond_t;
    typedef std::vector<cond_t> condition_vec_t;
    typedef std::vector<condition_vec_t> condition_vec2_t;

    typedef std::pair<map_iterator, map_iterator> map_iterator_range_t;
    typedef std::vector<map_iterator_range_t> map_iterator_range_list_t;

    struct map_range_iterator
    {
    public:
        map_iterator & get() { return it; }
        map_iterator const& get() const { return it; }

        map_range_iterator& operator++()
        {
            next();
            return *this;
        }

        bool next()
        {
            if (isEnd()) return false;
            ++it;
            if (it == range[idx].second) {
                if (idx == range.size() - 1) {
                    return false;
                }

                ++idx;
                it = range[idx].first;
            }
            return true;
        }

        bool isEnd() const {
            if (range.empty()) return true;
            return idx == range.size() - 1 && it == range[idx].second;
        }

        explicit operator bool() const { return !isEnd(); }

    public:
        map_iterator it;
        map_iterator_range_list_t range;
        size_t idx = 0;
    };

    struct iterator
    {
    public:
        iterator() = default;
        explicit iterator(this_t * t) : self(t) {}

        keys_t keys()
        {
            keys_t ks;
            ks.reserve(mapItrs.size());
            for (map_iterator & it : mapItrs)
                ks.push_back(it->first);
            return ks;
        }

        V const& get() { return *setItr; }

        iterator & operator++()
        {
            if (isEnd())
                return *this;

            assert(mapItrs.size() == self->depth_ + 1);
            set_t * s = reinterpret_cast<set_t*>(mapItrs.back()->second);
            ++setItr;
            if (s->end() != setItr)
                return *this;

            // 逐层回退, ++
            while (!mapItrs.empty()) {
                map_iterator & it = mapItrs.back();
                map_t* parent = (mapItrs.size() == 1) ? root()
                    : reinterpret_cast<map_t*>(mapItrs[mapItrs.size() - 2]->second);
                map_iterator last = parent->end();

                ++it;
                for (; it != last; ++it) {
                    if (begin(reinterpret_cast<map_t*>(it->second), mapItrs.size()))
                        // next完成
                        return *this;
                }

                // 本层遍历到底了, 需要回退
                mapItrs.pop_back();
            }

            setEnd();
            return *this;
        }

        iterator operator++(int)
        {
            iterator it = *this;
            ++(*this);
            return it;
        }

        explicit operator bool() const { return !isEnd(); }

        bool isEnd() const
        {
            return !self || mapItrs.empty();
        }

        friend bool operator==(iterator const& lhs, iterator const& rhs)
        {
            if (lhs.isEnd() && rhs.isEnd()) return true;

            if (lhs.self != rhs.self) return false;
            if (lhs.mapItrs.size() != rhs.mapItrs.size()) return false;
            for (size_t i = 0; i < lhs.mapItrs.size(); ++i)
                if (lhs.mapItrs[i] != rhs.mapItrs[i])
                    return false;

            return lhs.setItr == rhs.setItr;
        }

        friend bool operator!=(iterator const& lhs, iterator const& rhs)
        {
            return !(lhs == rhs);
        }

        virtual void setBegin()
        {
            if (!self) return ;

            mapItrs.clear();
            mapItrs.reserve(self->depth_ + 1);

            if (!begin(root(), 0)) {
                setEnd();
            }
        }

        virtual void setEnd()
        {
            if (!self) return ;

            mapItrs.clear();
            setItr = set_iterator{};
        }

    protected:
        inline map_t* root() const { return &self->m_; }

    private:
        virtual bool begin(map_t* m, size_t depth)
        {
            if (depth < self->depth_ + 1) {
                // 补全未命中索引
                map_iterator it = m->begin();
                if (m->end() == it)
                    return false;

                mapItrs.push_back(it);

                map_t * next = reinterpret_cast<map_t*>(it->second);
                if (begin(next, depth + 1))
                    return true;

                mapItrs.pop_back();
                return false;
            }

            // set
            // if (depth == self->depth_ + 1)
            set_t * s = reinterpret_cast<set_t*>(m);
            setItr = s->begin();
            return true;
        }

    private:
        friend struct RecursiveMapSet;
        this_t * self = nullptr;
        vector<map_iterator> mapItrs;
        set_iterator setItr;
    };

    struct condition_iterator
    {
    public:
        condition_iterator() = default;
        explicit condition_iterator(this_t * t,
                std::shared_ptr<condition_vec2_t> cv2,
                Debugger::IndexHintInfo* pIndexHintInfo)
            : self(t), condv2(cv2), indexHintInfo(pIndexHintInfo) {}

        condition_iterator(condition_iterator && other) = default;
        condition_iterator& operator=(condition_iterator && other) = default;

//        condition_iterator(condition_iterator && other) {
//            *this = std::move(other);
//        }
//        condition_iterator& operator=(condition_iterator && other) {
//            self = other.self;
//            setItr = other.setItr;
//            condv2 = other.condv2;
//            std::swap(mapRangeItrs, other.mapRangeItrs);
//            other.setEnd();
//            return *this;
//        }
        
        condition_iterator(condition_iterator const&) = delete;
        condition_iterator& operator=(condition_iterator const&) = delete;

        V & get() { return const_cast<V&>(*setItr); }
        V const& get() const { return *setItr; }

        condition_iterator & operator++()
        {
            next();
            return *this;
        }

        condition_iterator operator++(int)
        {
            condition_iterator it = *this;
            ++(*this);
            return it;
        }

        explicit operator bool() const { return !isEnd(); }

        bool isEnd() const
        {
            return !self || mapRangeItrs.empty();
        }

        friend bool operator==(condition_iterator const& lhs, condition_iterator const& rhs)
        {
            if (lhs.isEnd() && rhs.isEnd()) return true;

            if (lhs.self != rhs.self) return false;
            if (lhs.mapRangeItrs.size() != rhs.mapRangeItrs.size()) return false;
            for (size_t i = 0; i < lhs.mapRangeItrs.size(); ++i)
                if (lhs.mapRangeItrs[i].get() != rhs.mapRangeItrs[i].get())
                    return false;

            return lhs.setItr == rhs.setItr;
        }

        friend bool operator!=(condition_iterator const& lhs, condition_iterator const& rhs)
        {
            return !(lhs == rhs);
        }

        virtual void setBegin()
        {
            if (!self) return ;

            mapRangeItrs.clear();
            mapRangeItrs.reserve(self->depth_ + 1);

            if (!begin(root(), 0)) {
                setEnd();
            }
        }

        virtual void setEnd()
        {
            if (!self) return ;

            mapRangeItrs.clear();
            setItr = set_iterator{};
        }

    protected:
        map_t* root() const { return &self->m_; }

    private:
        // range交集
        void crossRange(map_t* m,
                map_iterator_range_list_t & rangeList,
                map_iterator_range_t const& range)
        {
            auto lt = [m](map_iterator const& lhs, map_iterator const& rhs){
                if (lhs == rhs) return false;
                if (lhs == m->end()) return false;
                if (rhs == m->end()) return true;
                return lhs->first < rhs->first;
            };
            auto le = [m](map_iterator const& lhs, map_iterator const& rhs){
                if (lhs == rhs) return false;
                if (lhs == m->end()) return false;
                if (rhs == m->end()) return true;
                return !(rhs->first < lhs->first);
            };

            auto pos = rangeList.begin();
            while (pos != rangeList.end()) {
                map_iterator_range_t & r = *pos;
                if (le(r.second, range.first) || le(range.second, r.first)) {
                    // 超出范围
                    pos = rangeList.erase(pos);
                    continue;
                }

                // 有交集
                if (lt(r.first, range.first)) {
                    r.first = range.first;
                }

                if (lt(range.second, r.second)) {
                    r.second = range.second;
                }

                if (r.first == r.second) { // 切空了
                    pos = rangeList.erase(pos);
                    continue;
                }

                ++pos;
            }
        }

        // ne(!=)
        void expectRange(map_t* m,
                map_iterator_range_list_t & rangeList,
                map_iterator const& neIter)
        {
            auto lt = [m](map_iterator const& lhs, map_iterator const& rhs){
                if (lhs == rhs) return false;
                if (lhs == m->end()) return false;
                if (rhs == m->end()) return true;
                return lhs->first < rhs->first;
            };
            auto cmp = [m, lt](map_iterator_range_t const& lhs, map_iterator const& rhs) {
                return lt(lhs.first, rhs);
            };

            // [left, right]两边剪枝
            typename map_iterator_range_list_t::iterator pos = std::lower_bound(
                    rangeList.begin(), rangeList.end(), neIter, cmp);
            if (pos->first == neIter) {    // 命中在首端
                ++pos->first;
                return ;
            }

            if (pos == rangeList.begin())   // pos前面没有了
                return ;

            --pos;
            assert(lt(pos->first, neIter));
            if (lt(neIter, pos->second)) {  // 命中在中间, 需要切分 
                map_iterator_range_t right{neIter, pos->second};
                ++right.first;

                pos->second = neIter;
                ++pos;
                rangeList.insert(pos, right);
            }
        }

        // @cond: {A > 1 && A < 3 && A != 2}
        //  (-, +) + {A > 1} = (1, +)
        map_iterator_range_list_t makeRanges(map_t* m, condition_vec_t const& cond)
        {
            map_iterator_range_list_t rangeList{{m->begin(), m->end()}};

            for (auto const& kv : cond)
            {
                K const& key = kv.first;
                e_cond_op op = kv.second;
                map_iterator_range_t range;

                switch ((int)op)
                {
                    case (int)e_cond_op::lt:
                        range.first = m->begin();
                        range.second = m->lower_bound(key);
                        crossRange(m, rangeList, range);
                        break;

                    case (int)e_cond_op::le:
                        range.first = m->begin();
                        range.second = m->upper_bound(key);
                        crossRange(m, rangeList, range);
                        break;

                    case (int)e_cond_op::eq:
                        range.second = range.first = m->find(key);
                        if (range.first != m->end()) {
                            ++range.second;
                        }
                        crossRange(m, rangeList, range);
                        break;

                    case (int)e_cond_op::ge:
                        range.first = m->lower_bound(key);
                        range.second = m->end();
                        crossRange(m, rangeList, range);
                        break;

                    case (int)e_cond_op::gt:
                        range.first = m->upper_bound(key);
                        range.second = m->end();
                        crossRange(m, rangeList, range);
                        break;

                    case (int)e_cond_op::ne:
                        // lt && gt
                        auto it = m->find(key);
                        if (it != m->end()) {
                            expectRange(m, rangeList, it);
                        }
                        break;
                }

                if (rangeList.empty()) {
                    // 交集已为空
                    break;
                }
            }

            return rangeList;
        }

        virtual bool begin(map_t* m, size_t depth)
        {
            if (depth < condv2->size()) {
                // 命中索引
                condition_vec_t const& cond = (*condv2)[depth];

                map_iterator_range_list_t range = makeRanges(m, cond);

                if (range.empty()) {
                    // 条件未命中索引, 回退上级索引向后迭代
                    return false;
                }

                mapRangeItrs.resize(mapRangeItrs.size() + 1);
                map_range_iterator & mrIter = mapRangeItrs.back();
                mrIter.range.swap(range);
                mrIter.it = mrIter.range[0].first;
                onScan();

                for (; mrIter; ++mrIter, onScan()) {
                    // 最后一次递归中, next其实是一个set_t*
                    map_t * next = reinterpret_cast<map_t*>(mrIter.get()->second);
                    if (begin(next, depth + 1)) {
                        return true;
                    }
                }

                mapRangeItrs.pop_back();
                return false;
            } else if (depth < self->depth_ + 1) {
                // 补全未命中索引
                assert(!m->empty());

                mapRangeItrs.resize(mapRangeItrs.size() + 1);
                map_range_iterator & mrIter = mapRangeItrs.back();
                mrIter.range.push_back({m->begin(), m->end()});
                mrIter.it = mrIter.range[0].first;
                onScan();

                map_t * next = reinterpret_cast<map_t*>(mrIter.get()->second);
                if (begin(next, depth + 1))
                    return true;

                mapRangeItrs.pop_back();
                return false;
            }

            // set
            // if (depth == self->depth_ + 1)
            set_t * s = reinterpret_cast<set_t*>(m);
            setItr = s->begin();
            onScan();
            return true;
        }

        void next()
        {
            if (isEnd())
                return ;

            assert(mapRangeItrs.size() == self->depth_ + 1);
            set_t * s = reinterpret_cast<set_t*>(mapRangeItrs.back().get()->second);
            ++setItr;
            onScan();
            if (s->end() != setItr)
                return ;

            // 逐层回退, ++
            while (!mapRangeItrs.empty()) {
                map_range_iterator & mrIter = mapRangeItrs.back();

                assert(!!mrIter);
                ++mrIter;
                onScan();
                for (; mrIter; ++mrIter, onScan()) {
                    if (begin(reinterpret_cast<map_t*>(mrIter.get()->second), mapRangeItrs.size()))
                        return ;
                }

                // 本层遍历到底了, 需要回退
                mapRangeItrs.pop_back();
                continue;
            }

            setEnd();
        }

        inline void onScan()
        {
            if (indexHintInfo) {
                ++indexHintInfo->nScanIndexKeys;
            }
        }

    private:
        this_t * self = nullptr;
        Debugger::IndexHintInfo* indexHintInfo = nullptr;
        set_iterator setItr;
        std::shared_ptr<condition_vec2_t> condv2; // size <= self->depth_ + 1
        std::vector<map_range_iterator> mapRangeItrs;
    };

    explicit RecursiveMapSet(size_t depth) : depth_(depth) {}
    ~RecursiveMapSet() { clear(); }

    iterator begin()
    {
        if (m_.empty()) {
            return end();
        }

        iterator it(this);
        it.setBegin();
        return it;
    }

    iterator end()
    {
        iterator it(this);
        it.setEnd();
        return it;
    }

    void set(keys_t const& keys, V const& value)
    {
        assert(keys.size() == depth_ + 1);
        if (keys.size() != depth_ + 1) {
            throw std::logic_error("RecursiveMapSet::set keys size not equal depth_+1");
        }

        map_iterator mapItr;
        void* pos = &m_;
        for (size_t i = 0; i < depth_ + 1; ++i) {
            map_t * m = reinterpret_cast<map_t*>(pos);
            mapItr = m->find(keys[i]);
            if (m->end() == mapItr) {
                void * p = (i == depth_) ? (void*)new set_t : (void*)new map_t;
                mapItr = m->insert({keys[i], p}).first;
            }
            pos = reinterpret_cast<void*>(mapItr->second);
        }

        // set
        set_t* s = reinterpret_cast<set_t*>(pos);
        if (s->erase(value)) {
            --size_;
        }
        s->insert(value);
        ++size_;
    }

    bool erase(keys_t const& keys, V const& value)
    {
        iterator it = find(keys, value);
        if (end() == it) {
            return false;
        }

        erase(it);
        return true;
    }

    iterator find(keys_t const& keys, V const& value)
    {
        assert(keys.size() == depth_ + 1);
        if (keys.size() != depth_ + 1) {
            throw std::logic_error("RecursiveMapSet::find keys size not equal depth_+1");
        }

        iterator it(this);
        void* pos = &m_;
        for (size_t i = 0; i < depth_ + 1; ++i) {
            map_t * m = reinterpret_cast<map_t*>(pos);
            map_iterator mapItr = m->find(keys[i]);
            if (m->end() == mapItr) {
                return end();
            }
            it.mapItrs.push_back(mapItr);
            pos = reinterpret_cast<void*>(mapItr->second);
        }

        // set
        set_t* s = reinterpret_cast<set_t*>(pos);
        it.setItr = s->find(value);
        if (s->end() == it.setItr) {
            return end();
        }

        return it;
    }

    void erase(iterator it)
    {
        assert(it.self == this);
        assert(it.mapItrs.size() == depth_ + 1);
        if (it.mapItrs.size() != depth_ + 1) {
            throw std::logic_error("RecursiveMapSet::erase it.mapItrs size not equal depth_+1");
        }

        for (size_t i = 0; i < depth_ + 1; ++i) {
            size_t ri = depth_ - i;
            map_iterator & mapItr = it.mapItrs[ri];
            map_t * parent = (ri == 0) ? &m_ : 
                reinterpret_cast<map_t*>(it.mapItrs[ri - 1]->second);

            if (i == 0) {   // 最后一个
                set_t* s = reinterpret_cast<set_t*>(mapItr->second);
                s->erase(it.setItr);
                --size_;
                if (s->empty()) {
                    delete s;
                    parent->erase(mapItr);
                }
            } else {
                map_t* m = reinterpret_cast<map_t*>(mapItr->second);
                if (m->empty()) {
                    delete m;
                    parent->erase(mapItr);
                }
            }
        }
    }

    bool empty()
    {
        return m_.empty();
    }

    size_t size()
    {
        return size_;
    }

    void clear()
    {
        clear(&m_, 0);
    }

    condition_iterator select(std::shared_ptr<condition_vec2_t> condv2,
            Debugger::IndexHintInfo* indexHintInfo)
    {
        assert(condv2->size() <= depth_ + 1);
        if (condv2->size() > depth_ + 1) {
            throw std::logic_error("RecursiveMapSet::begin too many conditions");
        }

        condition_iterator it(this, condv2, indexHintInfo);
        it.setBegin();
        return std::move(it);
    }

    string toString(bool simple = false)
    {
        string s;
        s += P("RecursiveMapSet [depth=%d keys.depth=%d size=%d]",
                (int)depth_, (int)depth_ + 1, (int)size());
        if (simple)
            return s;

        ++tlsTab();
        s += toString(&m_, 0);
        --tlsTab();
        return s;
    }

    string toString(map_t* m, size_t depth)
    {
        string s;
        if (depth < depth_ + 1) {
            // map
//            s += P("map[%d] [size=%d]", (int)depth, (int)m->size());
            ++tlsTab();
            int i = 0;
            for (auto it = m->begin(); it != m->end(); ++it, ++i)
            {
                s += P("[%d] -> K[%d]=%s", i, (int)depth, adl::to_string(it->first).c_str());
                ++tlsTab();
                map_t* next = reinterpret_cast<map_t*>(it->second);
                s += toString(next, depth + 1);
                --tlsTab();
            }
            --tlsTab();
            return s;
        }

        // set
        set_t * sp = reinterpret_cast<set_t*>(m);
//        s += P("set[%d] [size=%d]", (int)depth, (int)sp->size());
        ++tlsTab();
        int i = 0;
        for (auto it = sp->begin(); it != sp->end(); ++it, ++i)
        {
            s += P("[%d] V=%s", i, adl::to_string(*it).c_str());
        }
        --tlsTab();
        return s;
    }

private:
    void clear(map_t* m, size_t depth)
    {
        if (depth == depth_) {
            for (auto & kv : *m) {
                set_t * s = reinterpret_cast<set_t*>(kv.second);
                delete s;
            }
            m->clear();
            return ;
        }

        for (auto & kv : *m)
        {
            map_t * m = reinterpret_cast<map_t*>(kv.second);
            clear(m, depth + 1);
        }
        m->clear();
    }

private:
    size_t size_ = 0;
    size_t depth_ = 0;
    map_t m_;
};

// 索引结构
template <typename K, typename V>
struct ShadowRecursiveMapSet : public ShadowBase<K, V, RecursiveMapSet>
{
public:
    typedef ShadowBase<K, V, RecursiveMapSet> base_t;
    typedef typename base_t::VStorage VStorage;
    typedef typename base_t::table_t table_t;
    typedef typename base_t::table_ptr table_ptr;
    typedef typename base_t::table_t::cond_t cond_t;
    typedef typename base_t::table_t::condition_vec_t condition_vec_t;
    typedef typename base_t::table_t::condition_vec2_t condition_vec2_t;
    typedef typename base_t::table_t::keys_t keys_t;

    typedef std::pair<typename table_t::condition_iterator,
            typename table_t::condition_iterator> range_t;

    using base_t::reset;
    using base_t::logs;
    using base_t::level;
    using base_t::table;

    struct ordered_range_t
    {
        size_t level;
        range_t range;

        ordered_range_t() = default;

        VStorage & vs() { return range.first.get(); }
        VStorage const& vs() const { return range.first.get(); }
        
        friend bool operator<(ordered_range_t const& lhs, ordered_range_t const& rhs)
        {
            // VStorage < VStorage
            if (lhs.vs() < rhs.vs())
                return true;

            if (rhs.vs() < lhs.vs())
                return false;

            // 让底层的先弹出, 最后一个弹出的即为有效数据
            return rhs.level < lhs.level;
        }
    };

    struct ordered_range_ref
    {
        std::shared_ptr<std::vector<ordered_range_t>> ranges; 
        size_t index;

        ordered_range_t & ort() { return (*ranges)[index]; }
        ordered_range_t const& ort() const { return (*ranges)[index]; }

        friend bool operator<(ordered_range_ref const& lhs, ordered_range_ref const& rhs)
        {
            return lhs.ort() < rhs.ort();
        }
    };

    struct condition_iterator
    {
    public:
        condition_iterator() = default;
        condition_iterator(condition_iterator &&) = default;
        condition_iterator& operator=(condition_iterator &&) = default;

        condition_iterator(condition_iterator const&) = delete;
        condition_iterator& operator=(condition_iterator const&) = delete;

        // V == ref_t
        V& operator*() { return *value; }
        V* operator->() { return value; }

        condition_iterator & operator++()
        {
            next();
            return *this;
        }

        explicit operator bool() const { return !isEnd(); }

        bool isEnd() const
        {
            return q.empty() && !value;
        }

        void init()
        {
            for (size_t i = 0; i < ranges->size(); ++i)
            {
                ordered_range_ref orr;
                orr.ranges = ranges;
                orr.index = i;
                q.push(orr);
            }

            // 定位到第一个有效数据
            next();
        }

        void next()
        {
            bool succ = false;
            while (!q.empty() && !succ) {
                ordered_range_ref orr = q.top();
                q.pop();

                ordered_range_t & ort = orr.ort();

                VStorage & vs = ort.vs();

                if (!posV || *posV == static_cast<V&>(vs)) {
                    // 相同或首次遍历, 覆盖
                    posV = &vs;
                    deleted = vs.deleted;
                } else {
                    // 新数据, 保存旧数据
                    if (!deleted) { // 数据有效, 完成一步next
                        value = posV;
                        succ = true;
                    }

                    // 数据无效, 继续探索
                    posV = &vs;
                    deleted = vs.deleted;
                }

                // 步进, 重新放回堆
                ++ort.range.first;
                if (ort.range.first != ort.range.second) {
                    q.push(orr);
                }
            }

            if (succ) {
                return ;
            }

            // 队列排空, 还未发现新数据, posV即为新数据
            value = !deleted ? posV : nullptr;
            posV = nullptr;
            deleted = false;
        }

    public:
        std::shared_ptr<std::vector<ordered_range_t>> ranges;
        std::priority_queue<ordered_range_ref> q;
        V *value = nullptr;
        V *posV = nullptr;
        bool deleted = false;
    };

    explicit ShadowRecursiveMapSet(size_t depth) : depth_(depth)
    {
        reset();
    }

    void fork(ShadowRecursiveMapSet<K, V> & other)
    {
        assert(other.depth_ == depth_);
        base_t::fork(static_cast<base_t&>(other));
    }

    void set(keys_t const& keys, V const& value)
    {
        VStorage vs{value};
        logs()->set(keys, vs);
    }

    void del(keys_t const& keys, V const& value)
    {
        if (level() == 1) {
            logs()->erase(keys, value);
            return ;
        }

        // 查看logs以下的层中是否有key
        for (size_t i = 1; i < level(); ++i) {
            table_ptr & htp = table(i);
            auto it = htp->find(keys, value);
            if (it == htp->end())
                continue;

            VStorage const& vs = it.get();
            if (vs.deleted) {  // 上层已删除, 底层无需再看
                break;
            }

            // 底层有, logs追加一个deleted记录
            VStorage dvs{value};
            dvs.deleted = true;
            logs()->set(keys, dvs);
            return ;
        }

        // 底层没有or已删除, logs里直接删除即可
        logs()->erase(keys, value);
    }

    condition_iterator select(std::shared_ptr<condition_vec2_t> condv2,
            Debugger::IndexHintInfo* indexHintInfo)
    {
        condition_iterator it;
        it.ranges = std::make_shared<std::vector<ordered_range_t>>();

        if (indexHintInfo) {
            indexHintInfo->nForkLevels = level();
        }

        // 自顶向下, 逐层查找
        for (size_t i = 0; i < level(); ++i) {
            table_ptr & htp = table(i);
            ordered_range_t ort;
            ort.level = i;
            ort.range.first = htp->select(condv2, indexHintInfo);
            ort.range.second.setEnd();
            if (ort.range.first == ort.range.second)
                continue;

            it.ranges->emplace_back(std::move(ort));
        }

        it.init();
        return std::move(it);
    }

protected:
    virtual table_ptr makeTable() override
    {
        return std::make_shared<table_t>(depth_);
    }

    virtual void mergeTable(table_t & from, table_t & to) override   // 覆盖合并
    {
        for (auto it = from.begin(); it != from.end(); ++it)
        {
            VStorage const& vs = it.get();
            if (vs.deleted) {
                to.erase(it.keys(), vs);
            } else {
                to.set(it.keys(), vs);
            }
        }
    }

    virtual string tableToString(table_t & table, bool simple) override
    {
        string s;
        s = table.toString(simple);
        return s;
    }

private:
    size_t depth_;
};

// 可以operator<的any
class LessAny
{
public:
    enum class e_simple_types : char
    {
        e_unseted = 0,
        e_any = 1,
        e_signed_integer_64 = 2,
        e_unsigned_integer_64 = 3,
        e_string = 4,
        e_max = 5,
    };

    struct min_t {};
    struct max_t {};

    LessAny() : type_(e_simple_types::e_unseted) {}
    explicit LessAny(min_t) : type_(e_simple_types::e_unseted) {}
    explicit LessAny(max_t) : type_(e_simple_types::e_max) {}

    LessAny(LessAny const& other) {
        *this = other;
    }

    LessAny& operator=(LessAny const& other) {
        if (this == &other) return *this;

        type_ = other.type_;
        if (type_ == e_simple_types::e_any) {
            u_.p = other.u_.p->clone();
        } else if (type_ == e_simple_types::e_string) {
            str_ = other.str_;
        } else {
            u_ = other.u_;
        }
    }

    LessAny(LessAny && other) {
        *this = std::move(other);
    }

    LessAny& operator=(LessAny && other) {
        if (this == &other) return *this;

        type_ = other.type_;
        if (type_ == e_simple_types::e_any) {
            std::swap(u_.p, other.u_.p);
        } else if (type_ == e_simple_types::e_string) {
            swap(str_, other.str_);
        } else {
            std::swap(u_, other.u_);
        }

        other.type_ = e_simple_types::e_unseted;
    }

    ~LessAny() {
        reset();
    }

    friend bool operator<(LessAny const& lhs, LessAny const& rhs)
    {
        if (lhs.type_ != rhs.type_)
            return lhs.type_ < rhs.type_;

        switch ((char)lhs.type_) {
            case (char)e_simple_types::e_unseted:
                return &lhs < &rhs;

            case (char)e_simple_types::e_any:
                return lhs.u_.p->less(rhs.u_.p);

            case (char)e_simple_types::e_signed_integer_64:
                return lhs.u_.i64 < rhs.u_.i64;

            case (char)e_simple_types::e_unsigned_integer_64:
                return lhs.u_.u64 < rhs.u_.u64;

            case (char)e_simple_types::e_string:
            default:
                return lhs.str_ < rhs.str_;
        }
    }

    string toString()
    {
        switch ((char)type_) {
            case (char)e_simple_types::e_unseted:
                return "e_unseted";

            case (char)e_simple_types::e_any:
                return u_.p->toString();

            case (char)e_simple_types::e_signed_integer_64:
                return std::to_string(u_.i64);

            case (char)e_simple_types::e_unsigned_integer_64:
                return std::to_string(u_.u64);

            case (char)e_simple_types::e_string:
                return "\"" + str_ + "\"";

            case (char)e_simple_types::e_max:
                return "e_max";
        }
    }

    void reset()
    {
        if (type_ == e_simple_types::e_any) {
            delete u_.p;
        }
        str_.clear();
        type_ = e_simple_types::e_unseted;
    }

    template <typename T>
    void set(T const& t)
    {
        reset();
        setData(t, NULL);
    }

private:
    template <typename T>
    typename std::enable_if<std::is_signed<T>::value, void>::type
    setData(T i64, int*)
    {
        u_.i64 = i64;
        type_ = e_simple_types::e_signed_integer_64;
    }

    template <typename T>
    typename std::enable_if<std::is_unsigned<T>::value, void>::type
    setData(T u64, int*)
    {
        u_.u64 = u64;
        type_ = e_simple_types::e_unsigned_integer_64;
    }

    template <typename T>
    typename std::enable_if<std::is_same<T, std::string>::value, void>::type
    setData(T str, int*)
    {
        str_ = str;
        type_ = e_simple_types::e_string;
    }

    template <typename T>
    void setData(T any_t, ...)
    {
        u_.p = new storage<T>(any_t);
        type_ = e_simple_types::e_any;
    }

private:
    struct base_t
    {
        virtual ~base_t() {}
        virtual bool less(base_t* other) const = 0;
        virtual base_t* clone() const = 0;
        virtual string toString() = 0;
    };

    template <typename T>
    struct storage : public base_t
    {
        storage(T const& v) : value_(v) {}

        bool less(base_t* other) const override
        {
            storage<T>* o = reinterpret_cast<storage<T>*>(other);
            return value_ < o->value_;
        }

        base_t* clone() const override
        {
            return new storage(value_);
        }

        string toString() override
        {
            return adl::to_string(value_);
        }

        T value_;
    };

private:
    union {
        int64_t i64;
        uint64_t u64;
        base_t* p;
    } u_;
    e_simple_types type_;
    string str_;
};

template <typename V>
struct DB_Base
{
public:
    // 单列:值
    typedef LessAny column_value_t;

    // 单列:元信息
    struct column_t
    {
        size_t offset = -1;
        std::function<column_value_t(V const&)> getter;
        std::function<void(V const& from, V& to)> assign;
        std::function<string()> fieldname;

        column_t() = default;
        column_t(column_t const&) = default;
        column_t(column_t &&) = default;
        column_t& operator=(column_t const&) = default;
        column_t& operator=(column_t &&) = default;

        template <typename FieldType>
        column_t(FieldType V::* memptr)
        {
            offset = reinterpret_cast<size_t>(&(((V*)0) ->* memptr));
            getter = [memptr](V const& v) {
                LessAny la;
                la.set(v.*memptr);
                return la;
            };
            assign = [memptr](V const& from, V & to) {
                to.*memptr = from.*memptr;
            };
            fieldname = [memptr]() {
                return adl::field_to_string<V>(memptr);
            };
        }

        friend bool operator<(column_t const& lhs, column_t const& rhs) {
            return lhs.offset < rhs.offset;
        }

        friend bool operator==(column_t const& lhs, column_t const& rhs) {
            return lhs.offset == rhs.offset;
        }
    };

    // ------------------ 查询条件
    // 查询条件(单个)
    struct condition_t
    {
        e_cond_op op_;
        column_t col_;
        column_value_t colValue_;

        condition_t() = default;
        condition_t(condition_t const&) = default;
        condition_t(condition_t &&) = default;
        condition_t& operator=(condition_t const&) = default;
        condition_t& operator=(condition_t &&) = default;

        template <typename FieldType>
        condition_t(FieldType V::* memptr, e_cond_op op, FieldType value)
            : op_(op), col_(memptr)
        {
            colValue_.set(value);
        }

        string toString()
        {
            return fmt("Cond(&%s) %s %s",
                    col_.fieldname().c_str(), e_cond_op_2_str(op_),
                    colValue_.toString().c_str());
        }

        friend bool operator<(condition_t const& lhs, condition_t const& rhs) {
            return lhs.col_ < rhs.col_;
        }

        bool check(V const& value)
        {
            // todo: 优化成指针形式的getter
            column_value_t colValue = col_.getter(value);
            switch ((int)op_) {
            case (int)e_cond_op::lt:
                return colValue < colValue_;
            case (int)e_cond_op::le:
                return !(colValue_ < colValue);
            case (int)e_cond_op::eq:
                return !(colValue_ < colValue) && !(colValue < colValue_);
            case (int)e_cond_op::ge:
                return !(colValue < colValue_);
            case (int)e_cond_op::gt:
                return colValue_ < colValue;

            default:
            case (int)e_cond_op::ne:
                return (colValue_ < colValue) || (colValue < colValue_);
            }
        }
    };

    // 语法糖辅助类
    template <typename FieldType>
    struct field_t
    {
        FieldType V::* memptr_;

        explicit field_t(FieldType V::* memptr) : memptr_(memptr) {}

        friend condition_t operator<(field_t field, FieldType value)
        {
            return condition_t{field.memptr_, e_cond_op::lt, value};
        }

        friend condition_t operator<=(field_t field, FieldType value)
        {
            return condition_t{field.memptr_, e_cond_op::le, value};
        }

        friend condition_t operator==(field_t field, FieldType value)
        {
            return condition_t{field.memptr_, e_cond_op::eq, value};
        }

        friend condition_t operator>=(field_t field, FieldType value)
        {
            return condition_t{field.memptr_, e_cond_op::ge, value};
        }

        friend condition_t operator>(field_t field, FieldType value)
        {
            return condition_t{field.memptr_, e_cond_op::gt, value};
        }

        friend condition_t operator!=(field_t field, FieldType value)
        {
            return condition_t{field.memptr_, e_cond_op::ne, value};
        }
    };

    // 一组&&查询条件
    // {&A::a == 1 && &A::b < 2}
    // {Cond(&A::a) == 1 && Cond(&A::b) < 2}
    struct condition_and_group
    {
        vector<condition_t> and_;

        condition_and_group() = default;

        condition_and_group(condition_t && c1)
            : and_{c1} {}

        condition_and_group(condition_t && c1, condition_t && c2)
            : and_{c1, c2} {}

        string toString()
        {
            string s;
            int i = 0;
            for (condition_t & cond : and_) {
                ++i;
                s += cond.toString();
                if (i != and_.size())
                    s += " && ";
            }
            return s;
        }

        void sort()
        {
            std::sort(and_.begin(), and_.end());
        }

        bool check(V const& value)
        {
            for (condition_t & cond : and_) {
                if (!cond.check(value))
                    return false;
            }
            return true;
        }
    };

    // && ||复合查询条件
    // {&A::a == 1 && &A::b < 2 || &A::c > 3}
    // {Cond(&A::a) == 1 && Cond(&A::b) < 2 || Cond(&A::c) > 3}
    struct condition_or_group
    {
        vector<condition_and_group> or_;

        condition_or_group() = default;

        condition_or_group(condition_t && c1)
            : or_{condition_and_group{std::move(c1)}} {}

        condition_or_group(condition_and_group && g1)
            : or_{g1} {}

        condition_or_group(condition_and_group && g1, condition_and_group && g2)
            : or_{g1, g2}{}

        string toString()
        {
            string s;
            int i = 0;
            for (condition_and_group & and_group : or_) {
                ++i;
                s += and_group.toString();
                if (i != or_.size())
                    s += " || ";
            }
            return s;
        }
    };

    // -------- cond
    // cond && cond
    friend condition_and_group operator&&(condition_t && c1, condition_t && c2)
    {
        return condition_and_group(std::move(c1), std::move(c2));
    }

    // cond && and_group
    friend condition_and_group && operator&&(condition_t && c1, condition_and_group && c2)
    {
        return std::move(c2) && std::move(c1);
    }

    // cond || cond
    friend condition_or_group operator||(condition_t && c1, condition_t && c2)
    {
        return condition_or_group(condition_and_group(std::move(c1)),
                condition_and_group(std::move(c2)));
    }

    // cond || and_group
    friend condition_or_group operator||(condition_t && c1, condition_and_group && c2)
    {
        return std::move(c2) || std::move(c1);
    }

    // cond || or_group
    friend condition_or_group && operator||(condition_t && c1, condition_or_group && c2)
    {
        return std::move(c2) || std::move(c1);
    }

    // -------- and_group
    // and_group && cond
    friend condition_and_group && operator&&(condition_and_group && c1, condition_t && c2)
    {
        c1.and_.emplace_back(std::move(c2));
        return std::move(c1);
    }

    // and_group && and_group
    friend condition_and_group && operator&&(condition_and_group && c1, condition_and_group && c2)
    {
        for (condition_t & c : c2.and_) {
            c1.and_.emplace_back(std::move(c));
        }
        c2.and_.clear();
        return std::move(c1);
    }

    // and_group && or_group
    friend condition_or_group && operator&&(condition_and_group && c1, condition_or_group && c2)
    {
        return std::move(c2) && std::move(c1);
    }

    // and_group || cond
    friend condition_or_group operator||(condition_and_group && c1, condition_t && c2)
    {
        return condition_or_group(std::move(c1), condition_and_group(std::move(c2)));
    }

    // and_group || and_group
    friend condition_or_group operator||(condition_and_group && c1, condition_and_group && c2)
    {
        return condition_or_group(std::move(c1), std::move(c2));
    }

    // and_group || or_group
    friend condition_or_group && operator||(condition_and_group && c1, condition_or_group && c2)
    {
        return std::move(c2) || std::move(c1);
    }

    // -------- or_group
    // or_group && cond
    friend condition_or_group && operator&&(condition_or_group && c1, condition_t && c2)
    {
        for (condition_and_group & ag : c1.or_) {
            condition_t c = c2;
            std::move(ag) && std::move(c);
        }
        return std::move(c1);
    }

    // or_group && and_group
    friend condition_or_group && operator&&(condition_or_group && c1, condition_and_group && c2)
    {
        for (condition_and_group & ag : c1.or_) {
            condition_and_group c = c2;
            std::move(ag) && std::move(c);
        }
        return std::move(c1);
    }

    // or_group || cond
    friend condition_or_group && operator||(condition_or_group && c1, condition_t && c2)
    {
        c1.or_.emplace_back(std::move(condition_and_group(std::move(c2))));
        return std::move(c1);
    }

    // or_group || and_group
    friend condition_or_group && operator||(condition_or_group && c1, condition_and_group && c2)
    {
        c1.or_.emplace_back(std::move(c2));
        return std::move(c1);
    }

    // or_group || or_group
    friend condition_or_group && operator||(condition_or_group && c1, condition_or_group && c2)
    {
        c1.or_.emplace(c2.or_.begin(), c2.or_.end());
        return std::move(c1);
    }
    // ------------------ 查询条件

    // ------------------ 索引
    // 索引:元信息
    struct index_meta_t
    {
        vector<column_t> cols;

        friend bool operator<(index_meta_t const& lhs, index_meta_t const& rhs)
        {
            for (size_t i = 0; i < (std::min)(lhs.cols.size(), rhs.cols.size()); ++i)
            {
                if (lhs.cols[i] < rhs.cols[i]) return true;
                if (rhs.cols[i] < lhs.cols[i]) return false;
            }
            return lhs.cols.size() < rhs.cols.size();
        }

        friend bool operator==(index_meta_t const& lhs, index_meta_t const& rhs)
        {
            return !(lhs < rhs) && !(rhs < lhs);
        }

        string toString()
        {
            string s("{");
            for (size_t i = 0; i < cols.size(); ++i) {
                column_t & col = cols[i];
                string fieldname = col.fieldname();
                if (fieldname.empty()) {
                    s += fmt("offset=%d", (int)col.offset);
                } else {
                    s += fieldname;
                }
                if (i + 1 < cols.size())
                    s += ", ";
            }
            s += "}";
            return s;
        }
    };

    // 索引:值
    struct index_value_t
    {
        vector<column_value_t> values;

        static index_value_t min(size_t n)
        {
            index_value_t ivt;
            ivt.values.resize(n, column_value_t(LessAny::min_t{}));
            return ivt;
        }

        static index_value_t max(size_t n)
        {
            index_value_t ivt;
            ivt.values.resize(n, column_value_t(LessAny::max_t{}));
            return ivt;
        }

        friend bool operator<(index_value_t const& lhs, index_value_t const& rhs)
        {
            for (size_t i = 0; i < (std::min)(lhs.values.size(), rhs.values.size()); ++i)
            {
                if (lhs.values[i] < rhs.values[i]) return true;
                if (rhs.values[i] < lhs.values[i]) return true;
            }
            return lhs.values.size() < rhs.values.size();
        }

        friend bool operator==(index_value_t const& lhs, index_value_t const& rhs)
        {
            return !(lhs < rhs) && !(rhs < lhs);
        }
    };

};

struct Config
{
    size_t minBucketCount = 1024;
};

template <typename K, typename V>
struct DB : public DB_Base<V>
{
public:
    DB() : DB(Config{}) {}
    explicit DB(Config const& conf) : data_(conf.minBucketCount) {}

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    // 存储引擎
    typedef ShadowHashTable<K, V> data_table_t;
    typedef DB<K, V> this_t;
    typedef DB_Base<V> base_t;
    typedef typename data_table_t::ref_t ref_t;

    typedef typename base_t::column_value_t column_value_t;
    typedef typename base_t::column_t column_t;
    typedef typename base_t::condition_t condition_t;
    typedef typename base_t::condition_and_group condition_and_group;
    typedef typename base_t::condition_or_group condition_or_group;
    typedef typename base_t::index_meta_t index_meta_t;
    typedef typename base_t::index_value_t index_value_t;

    // V*
    class VRefPtr
    {
    public:
        VRefPtr() = default;
        VRefPtr(this_t* db, ref_t ref) : db_(db), ref_(ref) {}

        V const& operator*() const
        {
            return *p();
        }

        V const* operator->() const
        {
            return p();
        }

        explicit operator bool() const
        {
            return !!p();
        }

        string toString()
        {
            return fmt("VRefPtr{db:0x%p, ref:%s}",
                    (void*)db_, ref_.toString().c_str());
        }

    private:
        V* p() const
        {
            if (!db_) return nullptr;

            ref_t newRef;
            V* ptr = db_->data_.get(ref_, newRef);
            if (ptr) {
                ref_ = newRef;
            }
            return ptr;
        }

    private:
        this_t* db_ = nullptr;
        mutable ref_t ref_;
    };

    // 索引: 元信息+值+数据
    struct index_t
    {
        typedef ShadowRecursiveMapSet<column_value_t, ref_t> map_t;
        index_meta_t meta;
        map_t data;

        index_t(index_meta_t const& mt) : meta(mt), data(mt.cols.size() - 1) {
            assert(!mt.cols.empty());
        }

        struct condition_iterator
        {
        public:
            condition_iterator() = default;
            condition_iterator(condition_iterator &&) = default;
            condition_iterator(condition_iterator const&) = delete;
            condition_iterator& operator=(condition_iterator const&) = delete;

            ref_t & operator*() { return *it; }
            ref_t * operator->() { return &*it; }

            V* getValue()
            {
                return value;
            }

            condition_iterator & operator++()
            {
                next();
                return *this;
            }

            bool isEnd() const
            {
                return it.isEnd();
            }

            explicit operator bool() const { return !isEnd(); }

            void init()
            {
                while (!isEnd() && !checkAndRef()) {
                    ++it;
                    value = nullptr;
                }

                if (indexHintInfo && !isEnd()) {
                    ++indexHintInfo->nResultRows;
                }
            }

        private:
            bool checkAndRef()
            {
                ref_t & ref = *it;
                ref_t out;
                value = table->get(ref, out);
                assert(!!value);
                if (value) {
                    // 更新ref
                    std::swap(ref, out);
                }

                if (indexHintInfo) {
                    ++indexHintInfo->nScanRows;
                }
                return and_group.check(*value);
            }

            void next()
            {
                if (isEnd())
                    return ;

                ++it;
                value = nullptr;
                init();
            }

        public:
            data_table_t* table;
            Debugger::IndexHintInfo* indexHintInfo = nullptr;
            typename map_t::condition_iterator it;
            condition_and_group and_group; // 未命中的索引, 遍历搜索
            V* value;
        };

        void fork(index_t & other)
        {
            assert(other.meta == meta);
            data.fork(other.data);
        }

        void merge()
        {
            data.merge();
        }

        index_value_t createValue(V const& value)
        {
            index_value_t ivt;
            for (auto & col : meta.cols)
            {
                ivt.values.push_back(col.getter(value));
            }
            return ivt;
        }

        // @return: <最左连续匹配长度, 总匹配cond数量>
        std::pair<size_t, size_t> match(condition_and_group const& and_group)
        {
            bool isLeftMatchedStop = false;
            size_t nLeftMatched = 0;
            size_t nMatchedColumn = 0;
            vector<column_t> & cols = meta.cols;
            for (column_t & col : cols) {
                size_t offset = col.offset;

                bool matched = false;
                for (condition_t const& cond : and_group.and_) {
                    if (cond.col_.offset == offset)
                    {
                        nMatchedColumn++;
                        matched = true;
                    }
                }

                if (!matched) {
                    isLeftMatchedStop = true;
                }

                if (!isLeftMatchedStop) {
                    ++nLeftMatched;
                }
            }
            return {nLeftMatched, nMatchedColumn};
        }

        void setIndex(V const& value, ref_t const& ref)
        {
            index_value_t ivt = createValue(value);
            data.set(ivt.values, ref);
        }

        void delIndex(V const& value, ref_t const& ref)
        {
            index_value_t ivt = createValue(value);
            data.del(ivt.values, ref);
        }

        void updateIndex(V const& oldValue, V const& newValue, ref_t const& ref)
        {
            index_value_t oldIvt = createValue(oldValue);
            index_value_t newIvt = createValue(newValue);
            if (oldIvt == newIvt) {
                return ;
            }

            data.del(oldIvt.values, ref);
            data.set(newIvt.values, ref);
        }

        condition_iterator select(data_table_t* table,
                condition_and_group const& and_group,
                Debugger::IndexHintInfo* indexHintInfo)
        {
            std::vector<bool> mask(and_group.and_.size());

            vector<column_t> & cols = meta.cols;

            typedef typename map_t::cond_t cond_t;
            typedef typename map_t::condition_vec2_t condv2_t;
            typedef std::shared_ptr<condv2_t> condv2_ptr;
            condv2_ptr condv2 = std::make_shared<condv2_t>();
            condv2->resize(cols.size());

            // and_group按字段聚合, 转为便于查询的结构: condv2
            for (size_t i = 0; i < cols.size(); ++i) {
                column_t & col = cols[i];
                size_t offset = col.offset;

                for (size_t j = 0; j < and_group.and_.size(); ++j) {
                    condition_t const& cond = and_group.and_[j];
                    if (cond.col_.offset == offset) {
                        // matched
                        (*condv2)[i].emplace_back(cond_t{cond.colValue_, cond.op_});
                        mask[j] = true;
                    }
                }
            }

            condition_iterator iter;
            iter.it = data.select(condv2, indexHintInfo);
            iter.table = table;
            iter.indexHintInfo = indexHintInfo;
            for (size_t j = 0; j < mask.size(); ++j) {
                if (mask[j])
                    continue;

                iter.and_group.and_.push_back(and_group.and_[j]);
            }
            iter.init();
            return std::move(iter);
        }

        string toString(bool simple = false)
        {
            string s;
            s += P("index [cols.size()=%d]", (int)meta.cols.size());
            ++tlsTab();

            {
                s += P("cols:");
                ++tlsTab();
                for (size_t i = 0; i < meta.cols.size(); ++i) {
                    column_t & col = meta.cols[i];
                    s += P("[%d] %s \t(offset=%d)",
                            (int)i, col.fieldname().c_str(), (int)col.offset);
                }
                --tlsTab();
            }

            {
                s += P("index-data:");
                ++tlsTab();
                s += data.toString(simple);
                --tlsTab();
            }

            --tlsTab();
            return s;
        }
    };
    typedef std::shared_ptr<index_t> index_ptr_t;

    // 索引搜索树
    struct index_tree_t
    {
        map<index_meta_t, index_ptr_t> indexes;

        index_ptr_t insert(index_meta_t meta)
        {
            index_ptr_t index = std::make_shared<index_t>(meta);
            auto kv = indexes.insert({index->meta, index});
            if (!kv.second)
                return index_ptr_t{};

            return index;
        }

        void fork(index_tree_t & other)
        {
            other.indexes.clear();
            for (auto & kv : indexes) {
                index_ptr_t & self = kv.second;
                index_ptr_t index = std::make_shared<index_t>(self->meta);
                self->fork(*index);
                other.indexes[kv.first] = index;
            }
        }

        void merge()
        {
            for (auto & kv : indexes) {
                kv.second->merge();
            }
        }

        // 匹配一个最佳索引
        index_ptr_t match(condition_and_group const& and_group,
                Debugger::OnceIndexQueryTrace* onceQueryTrace)
        {
            index_ptr_t bestIndex = nullptr;
            std::pair<size_t, size_t> maxMatched {0, 0};

            for (auto & kv : indexes) {
                index_ptr_t & index = kv.second;
                auto matched = index->match(and_group);

                if (onceQueryTrace) {
                    onceQueryTrace->tryMatchIndexes.resize(onceQueryTrace->tryMatchIndexes.size() + 1);
                    Debugger::IndexHintInfo & indexHintInfo = onceQueryTrace->tryMatchIndexes.back();
                    indexHintInfo.indexName = index->meta.toString();
                    indexHintInfo.nLeftMatched = matched.first;
                    indexHintInfo.nMatchedCond = matched.second;
                    indexHintInfo.nForkLevels = index->data.level();
                }

                if (matched > maxMatched) {
                    maxMatched = matched;
                    bestIndex = index;

                    if (onceQueryTrace) {
                        onceQueryTrace->matched = onceQueryTrace->tryMatchIndexes.back();
                    }
                }
            }

            return bestIndex;
        }

        void foreach(std::function<void(index_ptr_t)> pred)
        {
            for (auto & kv : indexes)
                pred(kv.second);
        }

        void setIndex(V const& value, ref_t const& ref)
        {
            foreach([&](index_ptr_t index){
                    index->setIndex(value, ref);
                });
        }

        void delIndex(V const& value, ref_t const& ref)
        {
            foreach([&](index_ptr_t index){
                    index->delIndex(value, ref);
                });
        }

        void updateIndex(V const& oldValue, V const& newValue, ref_t const& ref)
        {
            foreach([&](index_ptr_t index){
                    index->updateIndex(oldValue, newValue, ref);
                });
        }

        string toString(bool simple = false)
        {
            string s;
            int i = 0;
            foreach([&](index_ptr_t index){
                    s += P("[%d]", i++);
                    ++tlsTab();
                    s += index->toString(simple);
                    --tlsTab();
                });
            return s;
        }
    };
    // ------------------ 索引

public:
    // 传入成员指针列表
    // 例如：
    //   DB<string, A> db;
    //   db.createIndex({&A::a});
    //   db.createIndex({&A::a, &A::b});
    bool createIndex(vector<column_t> const& cols) {
        std::set<column_t> unique;

        index_meta_t meta;

        for (column_t const& col : cols) {
            if (unique.insert(col).second)
                meta.cols.push_back(col);
        }

        index_ptr_t index = indexes_.insert(meta);
        if (!index) {
            // 索引已存在
            return false;
        }

        for (column_t const& col : meta.cols)
            indexedColumns_.insert({col.offset, index});

        data_.foreach([index](K const& key, ref_t const& ref, V* ptr){
                index->setIndex(*ptr, ref);
                return true;
            });

        return true;
    }

    // CRUD
    bool insert(K const& key, V const& value) {
        if (data_.get(key)) {
            return false;
        }

        data_.set(key, value);
        ref_t ref {key};
        indexes_.setIndex(value, ref);
        ++size_;
        return true;
    }

    bool update(K const& key, V const& value) {
        ref_t ref;
        V* oldValue = data_.get(key, ref);
        if (!oldValue) {
            return false;
        }

        indexes_.updateIndex(*oldValue, value, ref);
        *oldValue = value;
        return true;
    }

    // 只更新部分字段, 在索引比较多的场景下可以有效减少索引重置
    bool update(K const& key, vector<column_t> const& cols, V const& value) {
        ref_t ref;
        V* oldValue = data_.get(key, ref);
        if (!oldValue) {
            return false;
        }

        std::set<index_ptr_t> indexes;
        for (column_t const& col : cols) {
            auto range = indexedColumns_.equal_range(col.offset);
            for (auto it = range.first; it != range.second; ++it)
                indexes.insert(it->second);
        }

        for (index_ptr_t index : indexes)
            index->updateIndex(*oldValue, value, ref);

        for (column_t const& col : cols) {
            col.assign(value, *oldValue);
        }
        return true;
    }

    // @return: true:首次插入, false:数据已存在,执行变更操作
    bool set(K const& key, V const& value)
    {
        ref_t ref;
        V* oldValue = data_.get(key, ref);
        if (!oldValue) {
            data_.set(key, value);
            ref_t newRef {key};
            indexes_.setIndex(value, newRef);
            ++size_;
            return true;
        }

        indexes_.updateIndex(*oldValue, value, ref);
        *oldValue = value;
        return false;
    }

    size_t size() const {
        return size_;
    }

    bool empty() const {
        return !size();
    }

    VRefPtr get(K const& key)
    {
        ref_t ref{key};
        return VRefPtr(this, ref);
    }

    bool get(K const& key, V & out)
    {
        V* p = data_.get(key);
        if (!p) {
            return false;
        }

        out = *p;
        return true;
    }

    bool del(K const& key)
    {
        ref_t ref;
        V* oldValue = data_.get(key, ref);
        if (!oldValue) {
            return false;
        }

        indexes_.delIndex(*oldValue, ref);
        data_.del(key);
        --size_;
        return true;
    }

    // 兼容
//    void forEach(const std::function<void(K const& key, V const& value)> &cb) {
//        foreach([&](K const& key, V const& value){
//                    cb(key, value);
//                    return true;
//                });
//    }

    // 无序遍历所有数据
    // @return: 是否遍历完所有数据
    // @cb: return-是否继续遍历, 返回false可以提前结束
    bool foreach(const std::function<bool(K const& key, V const& value)> &cb) {
        return data_.foreach([cb](K const& key, ref_t const& ref, V* ptr){
                return cb(key, *ptr);
            });
    }

    // 逐个取出select结果的迭代器
    // 注意: 数据有修改时, 迭代器很可能会失效.
    // Debugger的生命期要长于condition_iterator
    struct condition_iterator
    {
    public:
        condition_iterator() = default;
        explicit condition_iterator(this_t * db) : db(db) {}
        
        condition_iterator(condition_iterator &&) = default;
        condition_iterator& operator=(condition_iterator &&) = default;

        condition_iterator(condition_iterator const&) = delete;
        condition_iterator& operator=(condition_iterator const&) = delete;

        std::pair<K const*, V const*> & operator*()
        {
            return value;
        }

        std::pair<K const*, V const*> * operator->()
        {
            return &value;
        }

        condition_iterator & operator++()
        {
            next();
            return *this;
        }

        bool isEnd() const { return bEnd; }

        explicit operator bool() const { return !isEnd(); }

    private:
        void setBegin()
        {
            if (!foreachConds.or_.empty()) {
                foreachIter = db->data_.begin();
            }
            bEnd = false;

            if (!begin())
                setEnd();
        }

        void setEnd() { bEnd = true; }

        bool begin()
        {
            while (!indexIters.empty()) {
                auto & it = indexIters.front();
                if (!it) {
                    indexIters.pop_front();
                    continue;
                }
                
                K const& k = it->key;
                V const* vp = it.getValue();
                if (!unique.insert(k).second) {
                    ++it;
                    continue;
                }

                value = {&k, vp};
                return true;
            }

            if (!foreachConds.or_.empty()) {
                while (foreachIter) {
                    K const& k = foreachIter->first;
                    V const* vp = static_cast<V const*>(&foreachIter->second);

                    if (foreachHintInfo) {
                        ++foreachHintInfo->nScanRows;
                    }

                    bool matched = false;
                    for (condition_and_group & and_group : foreachConds.or_) {
                        if (and_group.check(*vp))
                        {
                            matched = true;
                            if (foreachHintInfo) {
                                ++foreachHintInfo->nResultRows;
                            }
                            break;
                        }
                    }

                    if (!matched) {
                        ++foreachIter;
                        continue;
                    }

                    if (!unique.insert(k).second) {
                        ++foreachIter;
                        continue;
                    }

                    value = {&k, vp};
                    return true;
                }
            }

            return false;
        }

        void next()
        {
            if (!indexIters.empty()) {
                auto & it = indexIters.front();
                ++it;
            } else if (!foreachConds.or_.empty()) {
                ++foreachIter;
            } else {
                setEnd();
                return ;
            }

            if (!begin())
                setEnd();
        }

    private:
        friend this_t;
        this_t * db = nullptr;
        condition_or_group foreachConds;
        std::list<typename index_t::condition_iterator> indexIters;
        typename data_table_t::iterator foreachIter;
        Debugger::IndexHintInfo* foreachHintInfo = nullptr;

        std::set<K> unique;
        std::pair<K const*, V const*> value;
        bool bEnd = true;
    };

    condition_iterator select(condition_or_group cond, Debugger * dbg = nullptr)
    {
        Debugger::QueryTrace* queryTrace = dbg ? &dbg->queryTrace : nullptr;
        if (queryTrace) {
            queryTrace->or_cond = cond.toString();
        }

        optimize(cond);

        if (queryTrace) {
            queryTrace->optimizedCond = cond.toString();
        }

        Debugger::OnceIndexQueryTrace* lastForeachQueryTrace = nullptr;

        condition_iterator condIter(this);
        for (size_t i = 0; i < cond.or_.size(); ++i)
        {
            condition_and_group & and_group = cond.or_[i];

            Debugger::OnceIndexQueryTrace* onceQueryTrace = nullptr;
            Debugger::IndexHintInfo* matchedIndexHintInfo = nullptr;
            if (queryTrace) {
                onceQueryTrace = queryTrace->newQuery();
                onceQueryTrace->cond = and_group.toString();
                matchedIndexHintInfo = &onceQueryTrace->matched;
            }

            // 选索引
            index_ptr_t index = indexes_.match(and_group, onceQueryTrace);
            if (index) {
                // 在索引上搜索
                typename index_t::condition_iterator it = index->select(
                        &data_, and_group, matchedIndexHintInfo);
                if (it) {
                    condIter.indexIters.emplace_back(std::move(it));
                }
                continue;
            }

            // 后续遍历搜索
            lastForeachQueryTrace = onceQueryTrace;
            condIter.foreachConds.or_.push_back(and_group);
        }

        if (!condIter.foreachConds.or_.empty()) {
            if (queryTrace) {
                lastForeachQueryTrace->cond = condIter.foreachConds.toString();
                lastForeachQueryTrace->matched.indexName = "foreach";
                condIter.foreachHintInfo = &lastForeachQueryTrace->matched;
            }
        }

        condIter.setBegin();
        return std::move(condIter);
    }

    std::vector<V const*> selectVector(condition_or_group cond, Debugger * dbg = nullptr)
    {
        std::vector<V const*> result;
        condition_iterator it = select(cond, dbg);
        for (; it; ++it) {
            V const* p = it->second;
            result.push_back(p);
        }
        return result;
    }

    std::vector<V> selectVectorCopy(condition_or_group cond, Debugger * dbg = nullptr)
    {
        std::vector<V> result;
        condition_iterator it = select(cond, dbg);
        for (; it; ++it) {
            V const* p = it->second;
            result.push_back(*p);
        }
        return result;
    }

    std::map<K, V const*> selectMap(condition_or_group cond, Debugger * dbg = nullptr)
    {
        std::map<K, V const*> result;
        condition_iterator it = select(cond, dbg);
        for (; it; ++it) {
            result[*it->first] = it->second;
        }
        return result;
    }

    std::map<K, V> selectMapCopy(condition_or_group cond, Debugger * dbg = nullptr)
    {
        std::map<K, V> result;
        condition_iterator it = select(cond, dbg);
        for (; it; ++it) {
            result[*it->first] = *it->second;
        }
        return result;
    }

    std::vector<V const*> selectVectorOld(condition_or_group cond, Debugger * dbg = nullptr)
    {
        std::map<ref_t, V const*> m;

        Debugger::QueryTrace* queryTrace = dbg ? &dbg->queryTrace : nullptr;
        if (queryTrace) {
            queryTrace->or_cond = cond.toString();
        }

        optimize(cond);

        if (queryTrace) {
            queryTrace->optimizedCond = cond.toString();
        }

        for (condition_and_group & and_group : cond.or_)
        {
            Debugger::OnceIndexQueryTrace* onceQueryTrace = nullptr;
            if (queryTrace) {
                queryTrace->querys.resize(queryTrace->querys.size() + 1);
                onceQueryTrace = &queryTrace->querys.back();
            }

            if (onceQueryTrace) {
                onceQueryTrace->cond = and_group.toString();
            }

            Debugger::IndexHintInfo* matchedIndexHintInfo = onceQueryTrace ?
                &onceQueryTrace->matched : nullptr;

            // 选索引
            index_ptr_t index = indexes_.match(and_group, onceQueryTrace);
            if (index) {
                // 在索引上搜索
                typename index_t::condition_iterator it = index->select(
                        &data_, and_group, matchedIndexHintInfo);
                for (; it; ++it) {
                    m[*it] = it.getValue();
                }
                continue;
            }

            // 遍历搜索
            if (matchedIndexHintInfo) {
                matchedIndexHintInfo->indexName = "foreach";
            }

            foreach([&](K const& key, V const& value) {
                    if (matchedIndexHintInfo) {
                        ++ matchedIndexHintInfo->nScanRows;
                    }

                    if (and_group.check(value)) {
                        m[ref_t(key)] = &value;
                        if (matchedIndexHintInfo) {
                            ++ matchedIndexHintInfo->nResultRows;
                        }
                    }
                    return true;
                });
        }

        std::vector<V const*> result;
        for (auto & kv : m)
            result.push_back(kv.second);
        return result;
    }

    // todo:查询条件优化
    // case1: (A > 1 && A < 3)
    // case2: (A > 2 || A < 2)
    // case3: (A > 2 || B == 3) && (A < 3 || B == 3)
    // case4: (A > 1 && A < 3 && A > 2)
    // other cases...
    void optimize(condition_or_group & cond)
    {
    }

    void fork(DB<K, V> & other)
    {
        other.size_ = size_;
        data_.fork(other.data_);
        indexes_.fork(other.indexes_);
        other.initIndexedColumns();
    }

    size_t forkLevel()
    {
        return data_.level();
    }

    void merge()
    {
        data_.merge();
        indexes_.merge();
    }

    string toString(bool simple = false)
    {
        string s;
        s += P("[ShadowDB](0x%p)", (void*)this);
        ++tlsTab();

        {
            s += P("data:");
            ++tlsTab();
            s += data_.toString(simple);
            --tlsTab();
        }

        {
            s += P("indexes:");
            ++tlsTab();
            s += indexes_.toString(simple);
            --tlsTab();
        }

        --tlsTab();
        return s;
    }

private:
    void initIndexedColumns()
    {
        indexedColumns_.clear();
        indexes_.foreach([this](index_ptr_t index) {
                for (column_t const& col : index->meta.cols)
                    indexedColumns_.insert({col.offset, index});
            });
    }

private:
    data_table_t data_;                 // 原始数据
    index_tree_t indexes_;              // 索引
    multimap<size_t, index_ptr_t> indexedColumns_;      // 索引涉及的列
    size_t size_ = 0;
};

// condition语法糖
// ex:
//   Cond(&A::a) < 1
//   Cond(&A::a) < 1 && Cond(&A::b) == 2
template <typename FieldType, typename V>
typename DB_Base<V>::template field_t<FieldType>
Cond(FieldType V::* memptr)
{
    typedef typename DB_Base<V>::template field_t<FieldType> ft;
    return ft(memptr);
}

} // namespace shadow

#ifndef SHADOW_DB_NOT_EXPORT_COND
using shadow::Cond;
#endif
