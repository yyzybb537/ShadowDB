// ShadowDB ::fork ::merge
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

    // 创建索引
    db.createIndex({&ProcessInfo::scriptVersion});

    // 写入数据
    for (ProcessInfo & pi : gData) {
        db.set(pi.processUUID, pi);
    }

    // fork创建一个数据分支
    // db2和db会共享原有db中的数据层, 并设置为只读
    // db2和db都会变为2层结构的数据库(索引也会同步变为2层)
    // 后续新的数据修改会保存在各自的新层上
    db_t db2;
    db.fork(db2);

    // 支持多次fork
    // 随着fork次数越来越多, 数据层数也会越来越多(无修改的连续fork不会增加额外层数).
    // 层数多了以后, CURD的性能都会有小幅度下降
    db_t db3;
    db2.fork(db3);
    cout << "db3 level:" << db3.forkLevel() << endl;

    // 每个fork的数据库都是独立的、互不影响的
    // 对db3的数据修改, 不会影响db和db2
    db3.del("11becf19-97fe-4683-9b8e-fc52c933c7bc");

    // 使用merge, 可以把数据库变回单层数据结构, 但这可能带来很多拷贝, 请谨慎使用!
    db3.merge();

    return 0;
}
