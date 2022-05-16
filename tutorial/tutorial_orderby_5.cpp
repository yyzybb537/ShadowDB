// ShadowDB ::支持order by对查询结果进行排序
#include "ShadowDB.h"
#include <set>
#include <string>
#include <chrono>
#include <iostream>
#include <time.h>

using namespace std;

struct Id
{
    int v1;
    int v2;

    Id() = default;
    Id(int _v1, int _v2) : v1(_v1), v2(_v2) {}

    // 重载operator<即可作为索引字段
    friend bool operator<(Id const& lhs, Id const& rhs)
    {
        if (lhs.v1 != rhs.v1)
            return lhs.v1 < rhs.v1;
        return lhs.v2 < rhs.v2;
    }
};

struct ProcessInfo {
    Id id;
    string processUUID;
    string scriptName;
    int64_t scriptVersion;
    string scriptKey;
    string nodeName;
};

std::vector<ProcessInfo> gData = {
    {{0, 0}, "11becf19-97fe-4683-9b8e-fc52c933c7bc", "b.cc", 1, "mips-b-1", "192.168.0.1"},
    {{0, 1}, "521e60b5-f636-4c8e-af51-860ef7771adc", "b.cc", 3, "mips-b-1", "192.168.0.2"},
    {{0, 2}, "88b91109-2514-47c2-a0d1-1c73bc6b37dc", "a.cc", 1, "mips-a-1", "192.168.0.3"},
    {{1, 0}, "e4e730ae-e7e2-4f70-a1ba-1d3733459932", "a.cc", 8, "mips-a-2", "192.168.0.4"},
    {{1, 1}, "9df40b2d-d1db-4c83-819b-259686a21f31", "c.cc", 1, "mips-c-1", "192.168.0.2"},
    {{1, 2}, "3038fc64-19fc-4212-9a46-cdb248104e9c", "d.cc", 2, "mips-a-1", "192.168.0.3"},
    {{2, 0}, "59d11ac7-425a-42e9-b83d-c61e21a79d88", "d.cc", 1, "mips-a-1", "192.168.0.4"},
    {{2, 1}, "9e146f96-dff1-427e-9aca-fee5f81fbbb3", "d.cc", 1, "mips-a-1", "192.168.0.1"},
};

int main()
{
    // 创建ShadowDB类, 第一个模板参数是主键类型, 第二个模板参数是存储的数据结构体
    typedef ::shadow::DB<string, ProcessInfo> db_t;
    db_t db;

    // 创建联合索引
    db.createIndex({&ProcessInfo::scriptKey, &ProcessInfo::nodeName});

    // 写入数据
    for (ProcessInfo & pi : gData) {
        db.set(pi.processUUID, pi);
    }

    // 条件查询的第二个参数可以传入一个order by语句
    // 注意：order by排序字段要尽量匹配索引最左前缀, 
    //       才能用迭代器逐个取数据, 否则会把所有数据取完排序再返回.
    std::vector<ProcessInfo const*> result = db.selectVector(
            Cond(&ProcessInfo::nodeName) >= "192.168.0.3", OrderBy(&ProcessInfo::scriptKey));

    cout << result.size() << endl;
    int i = 0;
    for (auto pi : result)
        cout << "[" << i++ << "]" << pi->scriptKey << ", " << pi->nodeName << endl;

    return 0;
}
