#include "ShadowDB.h"
#include <set>
#include <string>
#include <chrono>
#include <iostream>
#include <time.h>
#include "gtest/gtest.h"

using namespace std;

struct Id
{
    int v1;
    int v2;

    Id() = default;
    Id(int _v1, int _v2) : v1(_v1), v2(_v2) {}

    friend bool operator<(Id const& lhs, Id const& rhs)
    {
        if (lhs.v1 != rhs.v1)
            return lhs.v1 < rhs.v1;
        return lhs.v2 < rhs.v2;
    }

    friend bool operator>=(Id const& lhs, Id const& rhs)
    {
        return !(rhs < lhs);
    }

    string toString() const
    {
        return shadow::fmt("{%d,%d}", v1, v2);
    }
};

struct IdGroup
{
    std::set<Id> ids;

    IdGroup() = default;
    IdGroup(std::initializer_list<Id> il) : ids(il) {}

    void insert(Id id) {
        ids.insert(id);
    }

    friend bool operator==(IdGroup const& lhs, IdGroup const& rhs)
    {
        if (lhs.ids.size() != rhs.ids.size()) return false;
        for (Id const& id : lhs.ids) {
            if (!rhs.ids.count(id))
                return false;
        }
        return true;
    }
};

struct ProcessInfo {
    Id id;
    string processUUID;
    string scriptName;
    int64_t scriptVersion;
    string scriptKey;
    string nodeName;

    bool check(ProcessInfo const& other) const {
        return id.v1 == other.id.v1 && id.v2 == other.id.v2 &&
            processUUID == other.processUUID &&
            scriptName == other.scriptName &&
            scriptVersion == other.scriptVersion &&
            scriptKey == other.scriptKey &&
            nodeName == other.nodeName;
    }

    string toString() const
    {
        return ::shadow::fmt("id=%s,script=\"%s\",version=%ld,key=\"%s\",node=\"%s\"",
                id.toString().c_str(), scriptName.c_str(), scriptVersion,
                scriptKey.c_str(), nodeName.c_str());
    }
    
    string toStringAll() const
    {
        return ::shadow::fmt("id=%s,uuid=\"%s\",script=\"%s\",version=%ld,key=\"%s\",node=\"%s\"",
                id.toString().c_str(), processUUID.c_str(), scriptName.c_str(),
                scriptVersion, scriptKey.c_str(), nodeName.c_str());
    }
};

SHADOW_DB_DEBUG_FIELD(ProcessInfo, id);
SHADOW_DB_DEBUG_FIELD(ProcessInfo, processUUID);
SHADOW_DB_DEBUG_FIELD(ProcessInfo, scriptName);
SHADOW_DB_DEBUG_FIELD(ProcessInfo, scriptVersion);
SHADOW_DB_DEBUG_FIELD(ProcessInfo, scriptKey);
SHADOW_DB_DEBUG_FIELD(ProcessInfo, nodeName);

struct ProcessInfoDrived : public ProcessInfo
{
    int drived;
};

void dump(std::vector<ProcessInfo const*> const& r);
size_t find(std::vector<ProcessInfo> const& data, ProcessInfo const* ppi);
void dumpFind(std::vector<ProcessInfo> const& data, std::vector<ProcessInfo const*> const& r);

std::vector<ProcessInfo> data1 = {
    {{0, 0}, "11becf19-97fe-4683-9b8e-fc52c933c7bc", "b.cc", 1, "mips-b-1", "192.168.0.1"},
    {{0, 1}, "521e60b5-f636-4c8e-af51-860ef7771adc", "b.cc", 3, "mips-b-1", "192.168.0.2"},
    {{0, 2}, "88b91109-2514-47c2-a0d1-1c73bc6b37dc", "a.cc", 1, "mips-a-1", "192.168.0.3"},
    {{1, 0}, "e4e730ae-e7e2-4f70-a1ba-1d3733459932", "a.cc", 8, "mips-a-2", "192.168.0.4"},
    {{1, 1}, "9df40b2d-d1db-4c83-819b-259686a21f31", "c.cc", 1, "mips-c-1", "192.168.0.2"},
    {{1, 2}, "3038fc64-19fc-4212-9a46-cdb248104e9c", "d.cc", 2, "mips-a-1", "192.168.0.3"},
    {{2, 0}, "59d11ac7-425a-42e9-b83d-c61e21a79d88", "d.cc", 1, "mips-a-1", "192.168.0.4"},
    {{2, 1}, "9e146f96-dff1-427e-9aca-fee5f81fbbb3", "d.cc", 1, "mips-a-1", "192.168.0.1"},
};
    
// overlapped data1
std::vector<ProcessInfo> data2 = {
    {{0, 0}, "11becf19-97fe-4683-9b8e-fc52c933c7bc", "b.js", 0, "mips", "10.0.3.3"},
    {{0, 1}, "521e60b5-f636-4c8e-af51-860ef7771adc", "b.js", 0, "mips", "10.0.3.4"},
    {{0, 2}, "88b91109-2514-47c2-a0d1-1c73bc6b37dc", "a.js", 0, "mips", "10.0.3.5"},
    {{1, 0}, "e4e730ae-e7e2-4f70-a1ba-1d3733459932", "a.js", 0, "mips", "10.0.3.6"},
    {{1, 1}, "9df40b2d-d1db-4c83-819b-259686a21f31", "c.js", 0, "mips", "10.0.3.7"},
    {{1, 2}, "3038fc64-19fc-4212-9a46-cdb248104e9c", "d.js", 0, "mips", "10.0.3.8"},
    {{2, 0}, "59d11ac7-425a-42e9-b83d-c61e21a79d88", "d.js", 0, "mips", "10.0.3.9"},
    {{2, 1}, "9e146f96-dff1-427e-9aca-fee5f81fbbb3", "d.js", 0, "mips", "10.0.3.0"},
};
    
// overlapped data1
std::vector<ProcessInfo> data3 = {
    {{0, 0}, "11becf19-97fe-4683-9b8e-fc52c933c7bc", "b.js", 1, "mips", "10.0.3.3"},
    {{0, 1}, "521e60b5-f636-4c8e-af51-860ef7771adc", "b.js", 2, "mips", "10.0.3.4"},
    {{0, 2}, "88b91109-2514-47c2-a0d1-1c73bc6b37dc", "a.js", 4, "mips", "10.0.3.5"},
    {{1, 0}, "e4e730ae-e7e2-4f70-a1ba-1d3733459932", "a.js", 6, "mips", "10.0.3.6"},
    {{1, 1}, "9df40b2d-d1db-4c83-819b-259686a21f31", "c.js", 3, "mips", "10.0.3.7"},
    {{1, 2}, "3038fc64-19fc-4212-9a46-cdb248104e9c", "d.js", 3, "mips", "10.0.3.8"},
    {{2, 0}, "59d11ac7-425a-42e9-b83d-c61e21a79d88", "d.js", 4, "mips", "10.0.3.9"},
    {{2, 1}, "9e146f96-dff1-427e-9aca-fee5f81fbbb3", "d.js", 1, "mips", "10.0.3.0"},
};
    
// alone data
std::vector<ProcessInfo> g1 = {
    {{10, 0}, "df25c488-06b0-4cda-b457-5f2f611db76a", "b.py", 11, "g1-mips", "10.1.3.3"},
    {{10, 1}, "9b0445e0-b906-4114-ae17-a04e0c972041", "b.py", 12, "g1-mips", "10.1.3.4"},
    {{10, 2}, "6c67d8f0-fda3-41dc-b331-da9662e40e3d", "a.py", 14, "g1-mips", "10.1.3.5"},
    {{11, 0}, "b4e388a4-49b7-4cbe-896c-d18219c734dc", "a.py", 16, "g1-mips", "10.1.3.6"},
    {{11, 1}, "35e14652-674d-44b4-92ef-cfd7a9b4ad81", "c.py", 13, "g1-mips", "10.1.3.7"},
    {{11, 2}, "d94a1ddc-e2ff-4c51-b5e6-283101544c7a", "d.py", 13, "g1-mips", "10.1.3.8"},
    {{12, 0}, "835f8f94-198d-46ee-8a01-89fdabd69385", "d.py", 14, "g1-mips", "10.1.3.9"},
    {{12, 1}, "8db669c3-2c71-4dd6-8e08-284c87497d84", "d.py", 11, "g1-mips", "10.1.3.0"},
};

// alone data
std::vector<ProcessInfo> g2 = {
    {{20, 0}, "3629e775-6bf8-4f36-902f-aa5cbcab0651", "b.java", 21, "g1-mips", "10.2.3.3"},
    {{20, 1}, "1ce9bd3e-5096-4357-9285-7191916e991f", "b.java", 22, "g1-mips", "10.2.3.4"},
    {{20, 2}, "f0b2d958-99b7-4a91-83e4-cd33807c34c0", "a.java", 24, "g1-mips", "10.2.3.5"},
    {{21, 0}, "4475ac30-658e-41e5-af94-98494a17cb76", "a.java", 26, "g1-mips", "10.2.3.6"},
    {{21, 1}, "59eb87e1-fc6b-4a32-b593-9b2c46dcf63b", "c.java", 23, "g1-mips", "10.2.3.7"},
    {{21, 2}, "8eeb7ac3-49d5-41f2-86ec-abfcc0d403de", "d.java", 23, "g1-mips", "10.2.3.8"},
    {{22, 0}, "336ee556-4c79-4124-86a3-92eac22e608d", "d.java", 24, "g1-mips", "10.2.3.9"},
    {{22, 1}, "92c31973-49d3-4ee5-9e83-039adde8975b", "d.java", 21, "g1-mips", "10.2.3.0"},
};

int64_t us()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
}
    
int64_t ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
}
    
using db_t = shadow::DB<string, ProcessInfo>;
using id_db_t = shadow::DB<Id, ProcessInfo>;

// ????????????
TEST(shadowdb, simple)
{
    db_t db;

    // ????????????, ??????????????????false
    EXPECT_TRUE(db.createIndex({&ProcessInfo::scriptVersion}));
    EXPECT_FALSE(db.createIndex({&ProcessInfo::scriptVersion}));
    EXPECT_TRUE(db.createIndex({&ProcessInfo::nodeName}));
    EXPECT_FALSE(db.createIndex({&ProcessInfo::nodeName}));

    // ??????????????????, ??????????????????false
    EXPECT_TRUE(db.createIndex({&ProcessInfo::id, &ProcessInfo::scriptVersion}));
    EXPECT_FALSE(db.createIndex({&ProcessInfo::id, &ProcessInfo::scriptVersion}));

    // ???????????? (move)
    for (ProcessInfo const& pi : data1) {
        ProcessInfo pic = pi;
        EXPECT_TRUE(db.set(pi.processUUID, std::move(pic)));
    }
    // ?????????????????????????????????
    EXPECT_TRUE(db.createIndex({&ProcessInfo::id}));

//    cout << db.toString() << endl;

    // ????????????, ??????
    for (ProcessInfo const& pi : data2) {
        EXPECT_FALSE(db.set(pi.processUUID, pi));
    }

//    cout << db.toString() << endl;

    // ?????????????????????
    for (ProcessInfo pi : data3) {
        pi.nodeName = "fatal";
        EXPECT_TRUE(db.update(pi.processUUID, {&ProcessInfo::scriptVersion}, pi));
    }

//    cout << db.toString() << endl;

    // ????????????
    size_t idx = 5;
    ProcessInfo const& pi = data3[idx];
    db_t::VRefPtr ref = db.get(pi.processUUID);
    EXPECT_TRUE(ref);
    EXPECT_TRUE(pi.check(*ref));

    ProcessInfo out;
    EXPECT_TRUE(db.get(pi.processUUID, out));
    EXPECT_TRUE(pi.check(out));

    EXPECT_FALSE(db.get("ced7cb19-cf04-4b0e-8314-3399d7bb2b96"));
    EXPECT_FALSE(db.get("ced7cb19-cf04-4b0e-8314-3399d7bb2b96", out));
    EXPECT_FALSE(db.get("4e375371-53d4-4dbf-bd11-2fa608e73a83", out));

    // ????????????
    shadow::Debugger dbg1;
    std::vector<ProcessInfo const*> r1 = db.selectVector(Cond(&ProcessInfo::id) == pi.id, &dbg1);
    EXPECT_EQ(r1.size(), 1);
    if (!r1.empty()) {
        EXPECT_TRUE(pi.check(*r1[0]));
    }
//    cout << "Debugger1:\n" << dbg1.toString() << endl;

    shadow::Debugger dbg2;

    std::vector<ProcessInfo const*> r2 = db.selectVector(Cond(&ProcessInfo::scriptVersion) >= 3 && Cond(&ProcessInfo::scriptVersion) < 5, &dbg2);
    EXPECT_EQ(r2.size(), 4);
    IdGroup expectIds {
        {0, 2},
        {1, 1},
        {1, 2},
        {2, 0}
    };
    IdGroup r2ids;
    for (ProcessInfo const* ppi : r2) {
        r2ids.insert(ppi->id);
    }
    EXPECT_TRUE(r2ids == expectIds);
    cout << "db:\n" << db.toString() << endl;
    cout << "Debugger2:\n" << dbg2.toString() << endl;

    // ??????????????? (???????????????foreach)
    {
        shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector({}, &dbg);
        EXPECT_EQ(r.size(), 8);
//        cout << "select({}) dbg:\n" << dbg.toString() << endl;
//        dump(r);
    }

//    // range
//    {
//        db_t::condition_range range = db.selectRange(Cond(&ProcessInfo::scriptVersion) >= 3 && Cond(&ProcessInfo::scriptVersion) < 5);
//        IdGroup expectIds {
//            {0, 2},
//            {1, 1},
//            {1, 2},
//            {2, 0}
//        };
//        IdGroup r2ids;
//        for (auto kv : range) {
//            ProcessInfo const* ppi = kv.second;
//            r2ids.insert(ppi->id);
//        }
//        EXPECT_TRUE(r2ids == expectIds);
//    }
}

// ??????
TEST(shadowdb, drived)
{
    using db_drived_t = shadow::DB<string, ProcessInfoDrived>;
    db_drived_t db;
    EXPECT_TRUE(db.createIndex({&ProcessInfoDrived::scriptVersion}));

    ::shadow::column_t<ProcessInfoDrived> colx(&ProcessInfoDrived::drived);

    ::shadow::column_t<ProcessInfo> col(&ProcessInfoDrived::scriptVersion);
    ::shadow::column_t<ProcessInfoDrived> dcol(col);

    Cond(&ProcessInfoDrived::scriptVersion) > 1;
    db.select(Cond(&ProcessInfoDrived::scriptVersion) > 1);
    db.select(Cond(&ProcessInfoDrived::scriptVersion) > 1 && Cond(&ProcessInfoDrived::scriptVersion) < 3);
    db.select(Cond(&ProcessInfoDrived::scriptVersion) > 1 && Cond(&ProcessInfoDrived::scriptVersion) < 3
            || Cond(&ProcessInfoDrived::scriptVersion) > 1 && Cond(&ProcessInfoDrived::scriptVersion) < 3);

//    db.select(Cond(&ProcessInfoDrived::scriptVersion) > 1 && Cond(&ProcessInfoDrived::drived) < 3);
    db.select(Cond(&ProcessInfoDrived::drived) > 1 && Cond(&ProcessInfoDrived::scriptVersion) < 3);

//    db.select(Cond(&ProcessInfoDrived::scriptVersion) > 1 && Cond(&ProcessInfoDrived::drived) < 3
//            || Cond(&ProcessInfoDrived::scriptVersion) > 1 && Cond(&ProcessInfoDrived::scriptVersion) < 3);
}

// VRefPtr: selectVectorRef/selectMapRef/VRefPtr(shared_ptr)
TEST(shadowdb, ref)
{
    db_t db;

    // ????????????, ??????????????????false
    EXPECT_TRUE(db.createIndex({&ProcessInfo::scriptVersion}));
    EXPECT_FALSE(db.createIndex({&ProcessInfo::scriptVersion}));
    EXPECT_TRUE(db.createIndex({&ProcessInfo::nodeName}));
    EXPECT_FALSE(db.createIndex({&ProcessInfo::nodeName}));

    // ??????????????????, ??????????????????false
    EXPECT_TRUE(db.createIndex({&ProcessInfo::id, &ProcessInfo::scriptVersion}));
    EXPECT_FALSE(db.createIndex({&ProcessInfo::id, &ProcessInfo::scriptVersion}));

    // ????????????
    for (ProcessInfo const& pi : data1) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }
    // ?????????????????????????????????
    EXPECT_TRUE(db.createIndex({&ProcessInfo::id}));

    cout << db.toString() << endl;

    // ????????????
    shadow::Debugger dbg1;
    std::vector<db_t::VRefPtr> r1 = db.selectVectorRef(
            Cond(&ProcessInfo::scriptVersion) > 1 && Cond(&ProcessInfo::scriptVersion) < 4,
            &dbg1);
    EXPECT_EQ(r1.size(), 2);
    EXPECT_TRUE(data1[5].check(*r1[0]));
    EXPECT_TRUE(data1[1].check(*r1[1]));
    cout << "*r1[0]: " << r1[0]->toStringAll() << endl;
    cout << "*r1[1]: " << r1[1]->toStringAll() << endl;
//    cout << "Debugger1:\n" << dbg1.toString() << endl;

    // ?????????????????????
    for (ProcessInfo pi : data3) {
        EXPECT_TRUE(db.update(pi.processUUID, pi));
    }

    cout << db.toString() << endl;
    EXPECT_TRUE(data3[5].check(*r1[0]));
    EXPECT_TRUE(data3[1].check(*r1[1]));
    cout << "*r1[0]: " << r1[0]->toStringAll() << endl;
    cout << "*r1[1]: " << r1[1]->toStringAll() << endl;

    std::shared_ptr<ProcessInfo> myPi(new ProcessInfo{data1[0]});
    db_t::VRefPtr selfHolder(myPi);
    EXPECT_EQ(selfHolder.get(), myPi.get());
    EXPECT_TRUE(selfHolder->check(*myPi));
}

// ?????????????????????
TEST(shadowdb, no_index)
{
    db_t db;

    // ????????????
    for (ProcessInfo const& pi : data1) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    ::shadow::Debugger dbg;
    std::vector<ProcessInfo const*> r = db.selectVector(Cond(&ProcessInfo::scriptVersion) > 1 && Cond(&ProcessInfo::scriptVersion) < 4, &dbg);
    EXPECT_EQ(r.size(), 2);
    std::set<string> uuidSet;
    for (auto ppi : r) {
        uuidSet.insert(ppi->processUUID);
    }
    EXPECT_TRUE(uuidSet.count(data1[1].processUUID));
    EXPECT_TRUE(uuidSet.count(data1[5].processUUID));

    EXPECT_EQ(dbg.queryTrace.querys.size(), 1);
    EXPECT_EQ(dbg.queryTrace.getScanRows(), db.size());
    EXPECT_EQ(dbg.queryTrace.getResultRows(), r.size());

    cout << dbg.toString() << endl;
}

// ??????????????? / ???????????????????????????
TEST(shadowdb, or_condition)
{
    db_t db;

    // ????????????
    for (ProcessInfo const& pi : data1) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    // && and ||
    {
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                Cond(&ProcessInfo::scriptVersion) > 1
                && Cond(&ProcessInfo::scriptVersion) < 4
                || Cond(&ProcessInfo::nodeName) >= "192.168.0.3"
                ,
                &dbg);
        EXPECT_EQ(r.size(), 5);

        std::set<string> uuidSet;
        for (auto ppi : r) {
            uuidSet.insert(ppi->processUUID);
        }
        EXPECT_TRUE(uuidSet.count(data1[1].processUUID));
        EXPECT_TRUE(uuidSet.count(data1[2].processUUID));
        EXPECT_TRUE(uuidSet.count(data1[3].processUUID));
        EXPECT_TRUE(uuidSet.count(data1[5].processUUID));
        EXPECT_TRUE(uuidSet.count(data1[6].processUUID));
        EXPECT_EQ(dbg.queryTrace.querys.size(), 2);
        EXPECT_EQ(dbg.queryTrace.getScanRows(), db.size());
        EXPECT_EQ(dbg.queryTrace.getResultRows(), 5);

        cout << "Result:" << endl;
        int i = 0;
        for (auto ppi : r) {
            cout << " " << ::shadow::fmt("[%d] %s", i++, ppi->toString().c_str()) << endl;
        }

        cout << dbg.toString() << endl;
    }

    // ( && and || ) && cond
    {
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                (Cond(&ProcessInfo::scriptVersion) > 1
                && Cond(&ProcessInfo::scriptVersion) < 4
                || Cond(&ProcessInfo::nodeName) >= "192.168.0.3")
                 && Cond(&ProcessInfo::nodeName) < "192.168.0.4"
                ,
                &dbg);
        EXPECT_EQ(r.size(), 3);
        std::set<string> uuidSet;
        for (auto ppi : r) {
            uuidSet.insert(ppi->processUUID);
        }
        EXPECT_TRUE(uuidSet.count(data1[1].processUUID));
        EXPECT_TRUE(uuidSet.count(data1[2].processUUID));
        EXPECT_TRUE(uuidSet.count(data1[5].processUUID));
        EXPECT_EQ(dbg.queryTrace.querys.size(), 2);
        EXPECT_EQ(dbg.queryTrace.getScanRows(), db.size());
        EXPECT_EQ(dbg.queryTrace.getResultRows(), 3);

        cout << "Result:" << endl;
        int i = 0;
        for (auto ppi : r) {
            cout << " " << ::shadow::fmt("[%d] %s", i++, ppi->toString().c_str()) << endl;
        }

        cout << dbg.toString() << endl;
    }

    // ( && and || ) && ( && )
    {
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                (
                    Cond(&ProcessInfo::scriptVersion) > 1
                    && Cond(&ProcessInfo::scriptVersion) < 4
                    || Cond(&ProcessInfo::nodeName) >= "192.168.0.3"
                ) && (
                    Cond(&ProcessInfo::nodeName) < "192.168.0.4"
                    && Cond(&ProcessInfo::nodeName) < "192.168.0.4"
                )
                ,
                &dbg);
        EXPECT_EQ(r.size(), 3);
        std::set<string> uuidSet;
        for (auto ppi : r) {
            uuidSet.insert(ppi->processUUID);
        }
        EXPECT_TRUE(uuidSet.count(data1[1].processUUID));
        EXPECT_TRUE(uuidSet.count(data1[2].processUUID));
        EXPECT_TRUE(uuidSet.count(data1[5].processUUID));
        EXPECT_EQ(dbg.queryTrace.querys.size(), 2);
        EXPECT_EQ(dbg.queryTrace.getScanRows(), db.size());
        EXPECT_EQ(dbg.queryTrace.getResultRows(), 3);

        cout << "Result:" << endl;
        int i = 0;
        for (auto ppi : r) {
            cout << " " << ::shadow::fmt("[%d] %s", i++, ppi->toString().c_str()) << endl;
        }

        cout << dbg.toString() << endl;
    }
}

// !=
TEST(shadowdb, ne)
{
    db_t db;

    // ????????????
    for (ProcessInfo const& pi : data3) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    // ?????????
    {
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                Cond(&ProcessInfo::scriptVersion) != 3
                && Cond(&ProcessInfo::scriptVersion) != 4,
                &dbg);
        EXPECT_EQ(r.size(), 4);
        std::set<string> uuidSet;
        for (auto ppi : r) {
            uuidSet.insert(ppi->processUUID);
        }
        EXPECT_TRUE(uuidSet.count(data3[0].processUUID));
        EXPECT_TRUE(uuidSet.count(data3[1].processUUID));
        EXPECT_TRUE(uuidSet.count(data3[3].processUUID));
        EXPECT_TRUE(uuidSet.count(data3[7].processUUID));
        EXPECT_EQ(dbg.queryTrace.querys.size(), 1);
        EXPECT_EQ(dbg.queryTrace.getScanRows(), db.size());
        EXPECT_EQ(dbg.queryTrace.getResultRows(), 4);

        cout << "Result:" << endl;
        int i = 0;
        for (auto ppi : r) {
            cout << " " << ::shadow::fmt("[%d] %s", i++, ppi->toString().c_str()) << endl;
        }

        cout << dbg.toString() << endl;
    }

    db.createIndex({&ProcessInfo::scriptVersion});

    // ????????? (1)
    {
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                Cond(&ProcessInfo::scriptVersion) != 3
                && Cond(&ProcessInfo::scriptVersion) != 4,
                &dbg);
        EXPECT_EQ(r.size(), 4);
        std::set<string> uuidSet;
        for (auto ppi : r) {
            uuidSet.insert(ppi->processUUID);
        }
        EXPECT_TRUE(uuidSet.count(data3[0].processUUID));
        EXPECT_TRUE(uuidSet.count(data3[1].processUUID));
        EXPECT_TRUE(uuidSet.count(data3[3].processUUID));
        EXPECT_TRUE(uuidSet.count(data3[7].processUUID));
        EXPECT_EQ(dbg.queryTrace.querys.size(), 1);
        EXPECT_EQ(dbg.queryTrace.getScanRows(), r.size());
        EXPECT_EQ(dbg.queryTrace.getResultRows(), r.size());

        cout << "Result:" << endl;
        int i = 0;
        for (auto ppi : r) {
            cout << " " << ::shadow::fmt("[%d] %s", i++, ppi->toString().c_str()) << endl;
        }

        cout << dbg.toString() << endl;

        cout << db.toString() << endl;
    }

    // ????????? (2)
    {
        db.createIndex({&ProcessInfo::scriptVersion});

        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                Cond(&ProcessInfo::scriptVersion) != 3
                && Cond(&ProcessInfo::scriptVersion) != 4
                && Cond(&ProcessInfo::scriptVersion) > 1
                && Cond(&ProcessInfo::scriptVersion) <= 6,
                &dbg);
        EXPECT_EQ(r.size(), 2);
        std::set<string> uuidSet;
        for (auto ppi : r) {
            uuidSet.insert(ppi->processUUID);
        }
        EXPECT_TRUE(uuidSet.count(data3[1].processUUID));
        EXPECT_TRUE(uuidSet.count(data3[3].processUUID));
        EXPECT_EQ(dbg.queryTrace.querys.size(), 1);
        EXPECT_EQ(dbg.queryTrace.getScanRows(), r.size());
        EXPECT_EQ(dbg.queryTrace.getResultRows(), r.size());

        cout << "Result:" << endl;
        int i = 0;
        for (auto ppi : r) {
            cout << " " << ::shadow::fmt("[%d] %s", i++, ppi->toString().c_str()) << endl;
        }

        cout << dbg.toString() << endl;

        cout << db.toString() << endl;
    }
}

// ??????fork????????? fork(data + index) * CURD
TEST(shadowdb, fork_simple)
{
    db_t db;
    EXPECT_TRUE(db.createIndex({&ProcessInfo::scriptKey}));

    // ????????????
    for (ProcessInfo const& pi : data1) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    EXPECT_EQ(db.forkLevel(), 1);

    db_t forked;
    db.fork(forked);
    EXPECT_EQ(db.forkLevel(), 2);
    EXPECT_EQ(forked.forkLevel(), 2);

    EXPECT_EQ(db.size(), data1.size());
    EXPECT_EQ(forked.size(), data1.size());

    // ???????????????
    ProcessInfo const& newPi = data2[0];
    forked.update(newPi.processUUID, newPi);
    db_t::VRefPtr dbDataPtr = db.get(newPi.processUUID);
    db_t::VRefPtr forkedDataPtr = forked.get(newPi.processUUID);
    EXPECT_TRUE(dbDataPtr->check(data1[0]));
    EXPECT_FALSE(dbDataPtr->check(data2[0]));
    EXPECT_TRUE(forkedDataPtr->check(data2[0]));

//    cout << "db:\n" << db.toString() << endl;
//    cout << "forked:\n" << forked.toString() << endl;

    // ???????????????
    for (ProcessInfo const& pi : g1) {
        EXPECT_TRUE(forked.set(pi.processUUID, pi));
    }

    EXPECT_EQ(db.size(), data1.size());
    EXPECT_EQ(forked.size(), data1.size() + g1.size());

    // ??????
    ProcessInfo const& pi = g1[3];
    EXPECT_FALSE(!!db.get(pi.processUUID));
    EXPECT_TRUE(!!forked.get(pi.processUUID));

//    cout << "db:\n" << db.toString() << endl;
//    cout << "forked:\n" << forked.toString() << endl;

    // -------- ??????fork
    db_t f2;
    forked.fork(f2);
    EXPECT_EQ(db.forkLevel(), 2);
    EXPECT_EQ(forked.forkLevel(), 3);
    EXPECT_EQ(f2.forkLevel(), 3);

    db_t f3;    // ??????????????????, forkLevel?????????
    f2.fork(f3);
    EXPECT_EQ(db.forkLevel(), 2);
    EXPECT_EQ(forked.forkLevel(), 3);
    EXPECT_EQ(f2.forkLevel(), 3);
    EXPECT_EQ(f3.forkLevel(), 3);

//    cout << "db:\n" << db.toString() << endl;
//    cout << "forked:\n" << forked.toString() << endl;
//    cout << "f2:\n" << f2.toString() << endl;
//    cout << "f3:\n" << f3.toString() << endl;

    // ???????????????
    for (ProcessInfo const& pi : g2) {
        EXPECT_TRUE(f2.set(pi.processUUID, pi));
    }

    EXPECT_TRUE(f2.createIndex({&ProcessInfo::id}));

    EXPECT_EQ(db.size(), data1.size());
    EXPECT_EQ(forked.size(), data1.size() + g1.size());
    EXPECT_EQ(f2.size(), data1.size() + g1.size() + g2.size());
    EXPECT_EQ(f3.size(), data1.size() + g1.size());

    // ??????????????????
    EXPECT_TRUE(forked.del(g1[0].processUUID));
    EXPECT_FALSE(forked.del(g1[0].processUUID));
    EXPECT_FALSE(forked.get(g1[0].processUUID));
    EXPECT_FALSE(db.get(g1[0].processUUID));
    EXPECT_TRUE(f3.get(g1[0].processUUID));

    // ??????????????????
    EXPECT_TRUE(forked.del(data1[0].processUUID));
    EXPECT_FALSE(forked.get(data1[0].processUUID));
    EXPECT_TRUE(db.get(data1[0].processUUID));
    EXPECT_TRUE(f3.get(data1[0].processUUID));

//    cout << "db:\n" << db.toString() << endl;
//    cout << "forked:\n" << forked.toString() << endl;
//    cout << "f2:\n" << f2.toString() << endl;
}

// ??????fork????????? (hash??????)
TEST(shadowdb, fork_hashpong)
{
    shadow::Config conf;
    conf.minBucketCount = 1;

    db_t db(conf);
    EXPECT_TRUE(db.createIndex({&ProcessInfo::scriptKey}));
    EXPECT_TRUE(db.createIndex({&ProcessInfo::scriptVersion, &ProcessInfo::id}));

    // ????????????
    for (ProcessInfo const& pi : data1) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    EXPECT_EQ(db.forkLevel(), 1);

    db_t forked;
    db.fork(forked);
    EXPECT_EQ(db.forkLevel(), 2);
    EXPECT_EQ(forked.forkLevel(), 2);

    EXPECT_EQ(db.size(), data1.size());
    EXPECT_EQ(forked.size(), data1.size());

    // ???????????????
    for (ProcessInfo const& pi : g1) {
        EXPECT_TRUE(forked.set(pi.processUUID, pi));
    }

    // ??????????????????
    EXPECT_TRUE(forked.del(g1[0].processUUID));
    EXPECT_FALSE(forked.get(g1[0].processUUID));
    EXPECT_FALSE(db.get(g1[0].processUUID));

    // ??????????????????
    EXPECT_TRUE(forked.del(data1[0].processUUID));
    EXPECT_FALSE(forked.get(data1[0].processUUID));
    EXPECT_TRUE(db.get(data1[0].processUUID));

//    cout << "db:\n" << db.toString() << endl;
//    cout << "forked:\n" << forked.toString() << endl;

    // merge
    string uuid = data1[1].processUUID;
    db_t::VRefPtr ref = forked.get(uuid);
    EXPECT_TRUE(ref);
    if (ref) {
        EXPECT_EQ(uuid, ref->processUUID);
    }
//    cout << ref.toString() << endl;

    forked.merge();
    EXPECT_TRUE(ref);
    if (ref) {
        EXPECT_EQ(uuid, ref->processUUID);
    }
//    cout << ref.toString() << endl;

//    cout << "merged forked:\n" << forked.toString() << endl;

    ::shadow::Debugger dbg1;
    std::vector<ProcessInfo const*> r1 = forked.selectVector(Cond(&ProcessInfo::scriptKey) == "g1-mips", &dbg1);
    EXPECT_EQ(r1.size(), 7);
    for (auto ppi : r1) {
        EXPECT_EQ(ppi->scriptKey, "g1-mips");
    }
//    cout << dbg1.toString() << endl;
//
//    cout << "flushed index forked:\n" << forked.toString() << endl;
}

// ??????????????????merge
TEST(shadowdb, fastMerge)
{
    db_t db;
    db.createIndex({&ProcessInfo::id});

    // ????????????
    for (ProcessInfo const& pi : data1) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    EXPECT_EQ(db.forkLevel(), 1);

    {
        db_t forked;
        db.fork(forked);
        EXPECT_EQ(db.forkLevel(), 2);
        EXPECT_EQ(forked.forkLevel(), 2);

        EXPECT_EQ(db.size(), data1.size());
        EXPECT_EQ(forked.size(), data1.size());
    }

    // ???????????????
    for (ProcessInfo const& pi : g1) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

//    cout << "db:\n" << db.toString() << endl;
    db.merge();
    EXPECT_EQ(db.forkLevel(), 1);
//    cout << "merged db:\n" << db.toString() << endl;
}

// ?????????function??????????????????
// ??????????????????????????????????????????
TEST(shadowdb, VirtualColumn_Index_Select)
{
    db_t db;
    ::shadow::VirtualColumn<ProcessInfo, int64_t> scriptVersionMod3 = 
        db.makeVirtualColumn<int64_t>([](ProcessInfo const& pi) { return pi.scriptVersion % 3; }, "scriptVersionMod3");
    db.createIndex({scriptVersionMod3});

    ::shadow::VirtualColumn<ProcessInfo, int64_t> scriptVersionMod2 = 
        db.makeVirtualColumn<int64_t>([](ProcessInfo const& pi) { return pi.scriptVersion % 2; }, "scriptVersionMod2");

    // ????????????
    for (ProcessInfo const& pi : data1) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    EXPECT_EQ(db.forkLevel(), 1);

    {
        db_t forked;
        db.fork(forked);
        EXPECT_EQ(db.forkLevel(), 2);
        EXPECT_EQ(forked.forkLevel(), 2);

        EXPECT_EQ(db.size(), data1.size());
        EXPECT_EQ(forked.size(), data1.size());
    }

    // simple
    {
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(Cond(scriptVersionMod3) > 1, &dbg);
        EXPECT_EQ(r.size(), 2);

        cout << "--------------------------- simple" << endl;
        cout << db.toString() << endl;
        cout << dbg.toString() << endl;
        cout << "--------------------------- simple" << endl;
    }

    // not
    {
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(!(Cond(scriptVersionMod3) > 1), &dbg);
        EXPECT_EQ(r.size(), 6);

        cout << "--------------------------- not" << endl;
        cout << db.toString() << endl;
        cout << dbg.toString() << endl;
        cout << "--------------------------- not" << endl;
    }

    // ! &&
    {
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                !(Cond(scriptVersionMod3) > 1) && (Cond(scriptVersionMod2) == 1)
                , &dbg);
        EXPECT_EQ(r.size(), 6);

        cout << "--------------------------- ! and &&" << endl;
        cout << db.toString() << endl;
        cout << dbg.toString() << endl;
        cout << "--------------------------- ! and &&" << endl;
    }

    // && ||
    {
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                !(Cond(scriptVersionMod3) > 1) && (Cond(scriptVersionMod2) == 1)
                || (Cond(&ProcessInfo::scriptVersion) == 8)
                , &dbg);
        EXPECT_EQ(r.size(), 7);

        cout << "--------------------------- && and ||" << endl;
        cout << db.toString() << endl;
        cout << dbg.toString() << endl;
        cout << "--------------------------- && and ||" << endl;
    }
}

// order by
TEST(shadowdb, order_by)
{
    db_t db;
    ::shadow::VirtualColumn<ProcessInfo, int64_t> scriptVersionMod3 = 
        db.makeVirtualColumn<int64_t>([](ProcessInfo const& pi) { return pi.scriptVersion % 3; }, "scriptVersionMod3");
    db.createIndex({scriptVersionMod3});

    db.createIndex({&ProcessInfo::scriptName, &ProcessInfo::scriptVersion});
    db.createIndex({&ProcessInfo::scriptName, scriptVersionMod3});
    db.createIndex({&ProcessInfo::nodeName});

    // ????????????
    for (ProcessInfo const& pi : data1) {
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    // 1.order by????????????
    // 1.1 simple??????
    {
        cout << "------------------- 1.1" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                Cond(&ProcessInfo::nodeName) > "192.168.0.2",
                OrderBy(&ProcessInfo::nodeName), &dbg);
        EXPECT_EQ(r.size(), 4);
        EXPECT_EQ(r[0]->nodeName, "192.168.0.3");
        EXPECT_EQ(r[1]->nodeName, "192.168.0.3");
        EXPECT_EQ(r[2]->nodeName, "192.168.0.4");
        EXPECT_EQ(r[3]->nodeName, "192.168.0.4");

//        cout << dbg.toString() << endl;
//        dump(r);
    }

    // 1.2 function??????
    {
        cout << "------------------- 1.2" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                Cond(scriptVersionMod3) >= 1,
                OrderBy(scriptVersionMod3), &dbg);
        EXPECT_EQ(r.size(), 7);
        EXPECT_EQ(r[0]->scriptVersion % 3, 1);
        EXPECT_EQ(r[1]->scriptVersion % 3, 1);
        EXPECT_EQ(r[2]->scriptVersion % 3, 1);
        EXPECT_EQ(r[3]->scriptVersion % 3, 1);
        EXPECT_EQ(r[4]->scriptVersion % 3, 1);
        EXPECT_EQ(r[5]->scriptVersion % 3, 2);
        EXPECT_EQ(r[6]->scriptVersion % 3, 2);

//        cout << dbg.toString() << endl;
//        dump(r);
    }

    // 1.3 ???????????????????????????
    {
        cout << "------------------- 1.3" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                {},
                OrderBy(&ProcessInfo::scriptName), &dbg);
        EXPECT_EQ(r.size(), 8);
//        EXPECT_EQ(r[0]->nodeName, "192.168.0.3");

//        cout << db.toString() << endl;
//        cout << dbg.toString() << endl;
//        dump(r);
    }

    // 1.4 ??????order by
    {
        cout << "------------------- 1.4" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                {},
                OrderBy(&ProcessInfo::scriptName, &ProcessInfo::scriptVersion), &dbg);
        EXPECT_EQ(r.size(), 8);
//        EXPECT_EQ(r[0]->nodeName, "192.168.0.3");

//        cout << db.toString() << endl;
//        cout << dbg.toString() << endl;
//        dump(r);
    }

    // 1.5 (memptr + function)??????order by
    {
        cout << "------------------- 1.5" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                {},
                OrderBy(&ProcessInfo::scriptName, scriptVersionMod3), &dbg);
        EXPECT_EQ(r.size(), 8);
        EXPECT_TRUE(r[0]->check(data1[2]));
        EXPECT_TRUE(r[1]->check(data1[3]));
        EXPECT_TRUE(r[2]->check(data1[1]));
        EXPECT_TRUE(r[3]->check(data1[0]));
        EXPECT_TRUE(r[4]->check(data1[4]));
        EXPECT_TRUE(r[5]->check(data1[6]));
        EXPECT_TRUE(r[6]->check(data1[7]));
        EXPECT_TRUE(r[7]->check(data1[5]));

//        cout << db.toString() << endl;
//        cout << dbg.toString() << endl;
//        dump(r);
    } 
    
    // 1.6 order by?????????, ?????????????????????????????????
    {
        cout << "------------------- 1.6" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                Cond(scriptVersionMod3) >= 1,
                OrderBy(&ProcessInfo::scriptKey), &dbg);
        EXPECT_EQ(r.size(), 7);
        EXPECT_TRUE(r[0]->check(data1[6]));
        EXPECT_TRUE(r[1]->check(data1[2]));
        EXPECT_TRUE(r[2]->check(data1[7]));
        EXPECT_TRUE(r[3]->check(data1[5]));
        EXPECT_TRUE(r[4]->check(data1[3]));
        EXPECT_TRUE(r[5]->check(data1[0]));
        EXPECT_TRUE(r[6]->check(data1[4]));

//        cout << db.toString() << endl;
//        cout << dbg.toString() << endl;
//        dump(r);
//        dumpFind(data1, r);
    }

    // 1.7 ????????????, ???????????????
    {
        cout << "------------------- 1.7" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                Cond(scriptVersionMod3) >= 1,
                OrderBy(&ProcessInfo::scriptVersion, &ProcessInfo::scriptName), &dbg);
        EXPECT_EQ(r.size(), 7);
        EXPECT_TRUE(r[0]->check(data1[2]));
        EXPECT_TRUE(r[1]->check(data1[0]));
        EXPECT_TRUE(r[2]->check(data1[4]));
        EXPECT_TRUE(r[3]->check(data1[6]));
        EXPECT_TRUE(r[4]->check(data1[7]));
        EXPECT_TRUE(r[5]->check(data1[5]));
        EXPECT_TRUE(r[6]->check(data1[3]));

//        cout << db.toString() << endl;
//        cout << dbg.toString() << endl;
//        dump(r);
//        dumpFind(data1, r);
    }

    // 2.order by???????????????
    
    // 2.1 ?????????
    {
        cout << "------------------- 2.1" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                Cond(&ProcessInfo::scriptKey) >= "mips-a-2",
                OrderBy(&ProcessInfo::scriptKey), &dbg);
        EXPECT_EQ(r.size(), 4);
        EXPECT_TRUE(r[0]->check(data1[3]));
        EXPECT_TRUE(r[1]->check(data1[1]));
        EXPECT_TRUE(r[2]->check(data1[0]));
        EXPECT_TRUE(r[3]->check(data1[4]));

//        cout << db.toString() << endl;
//        cout << dbg.toString() << endl;
//        dump(r);
//        dumpFind(data1, r);
    }

    // 2.2 ????????????,?????????????????????
    {
        cout << "------------------- 2.2" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                {},
                OrderBy(&ProcessInfo::scriptVersion), &dbg);
        EXPECT_EQ(r.size(), 8);
        EXPECT_TRUE(r[0]->check(data1[0]));
        EXPECT_TRUE(r[1]->check(data1[7]));
        EXPECT_TRUE(r[2]->check(data1[2]));
        EXPECT_TRUE(r[3]->check(data1[6]));
        EXPECT_TRUE(r[4]->check(data1[4]));
        EXPECT_TRUE(r[5]->check(data1[5]));
        EXPECT_TRUE(r[6]->check(data1[1]));
        EXPECT_TRUE(r[7]->check(data1[3]));

//        cout << db.toString() << endl;
//        cout << dbg.toString() << endl;
//        dump(r);
//        dumpFind(data1, r);
    }

    // 2.3 ????????????, ???????????????
    {
        cout << "------------------- 2.3" << endl;
        ::shadow::Debugger dbg;
        std::vector<ProcessInfo const*> r = db.selectVector(
                {},
                OrderBy(&ProcessInfo::scriptVersion, &ProcessInfo::scriptName), &dbg);
        EXPECT_EQ(r.size(), 8);
        EXPECT_TRUE(r[0]->check(data1[2]));
        EXPECT_TRUE(r[1]->check(data1[0]));
        EXPECT_TRUE(r[2]->check(data1[4]));
        EXPECT_TRUE(r[3]->check(data1[7]));
        EXPECT_TRUE(r[4]->check(data1[6]));
        EXPECT_TRUE(r[5]->check(data1[5]));
        EXPECT_TRUE(r[6]->check(data1[1]));
        EXPECT_TRUE(r[7]->check(data1[3]));

//        cout << db.toString() << endl;
//        cout << dbg.toString() << endl;
//        dump(r);
//        dumpFind(data1, r);
    }
}

void dump(std::vector<ProcessInfo const*> const& r)
{
    cout << "->r:" << endl;
    int i = 0;
    for (auto ppi : r)
    {
        cout << ::shadow::fmt("[%d] %s", i++, ppi->toStringAll().c_str()) << endl;
    }
}

size_t find(std::vector<ProcessInfo> const& data, ProcessInfo const* ppi)
{
    for (size_t i = 0; i < data.size(); ++i)
    {
        if (data[i].check(*ppi))
            return i;
    }
    return -1;
}

void dumpFind(std::vector<ProcessInfo> const& data, std::vector<ProcessInfo const*> const& r)
{
    cout << "->find:" << endl;
    int i = 0;
    for (auto ppi : r)
    {
        cout << ::shadow::fmt("[%d] %d", i++, (int)find(data, ppi)) << endl;
    }
}

void initDB(db_t & db, int nRows, bool bCreateIndex = true, int nLevel = 1)
{
    for (int i = 0; i < nRows; ++i) {
        ProcessInfo pi;
        pi.id = {0, i};
        pi.processUUID = std::to_string(i);
        pi.scriptName = std::to_string(i) + ".cc";
        pi.scriptVersion = i;
        pi.scriptKey = "";
        pi.nodeName = "10." + std::to_string(i);
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    if (bCreateIndex) {
        db.createIndex({&ProcessInfo::id});
        db.createIndex({&ProcessInfo::nodeName});
        db.createIndex({&ProcessInfo::scriptName, &ProcessInfo::scriptVersion});
    }

    nLevel = (std::max)(nLevel, 1);
    for (int lv = 1; lv < nLevel; ++lv) {
        db_t temp;
        db.fork(temp);

        int i = nRows + lv;
        ProcessInfo pi;
        pi.id = {0, i};
        pi.processUUID = std::to_string(i);
        pi.scriptName = std::to_string(i) + ".cc";
        pi.scriptVersion = i;
        pi.scriptKey = "";
        pi.nodeName = "10." + std::to_string(i);
        EXPECT_TRUE(db.set(pi.processUUID, pi));
    }

    EXPECT_EQ(db.forkLevel(), nLevel);
}

// ????????????, ??????fork??????
TEST(shadowdb_bench, fork_large)
{
    vector<int> cs = {
        1000,
        10 * 1000,
        100 * 1000,
//        1000 * 1000,
    };

    cout << "benchmark: DB<string, ProcessInfo> with 3 indexes" << endl;
    for (int c : cs) {
        cout << "====================================" << endl;
        int64_t t1, t2, t3;
        int64_t beforeDestory, afterDestroy;
        {
            db_t db;
            db.createIndex({&ProcessInfo::id});
            db.createIndex({&ProcessInfo::nodeName});
            db.createIndex({&ProcessInfo::scriptName, &ProcessInfo::scriptVersion});

            t1 = us();
            for (int i = 0; i < c; ++i) {
                ProcessInfo pi;
                pi.id = {0, i};
                pi.processUUID = std::to_string(i);
                pi.scriptName = std::to_string(i) + ".cc";
                pi.scriptVersion = i;
                pi.scriptKey = "";
                pi.nodeName = "10." + std::to_string(i);
                EXPECT_TRUE(db.set(pi.processUUID, pi));
            }
            EXPECT_EQ(db.size(), c);
            t2 = us();
            cout << "insert " << c << " rows. cost: " << t2 - t1 << "us" << endl;

//            cout << "db(simple):\n" << db.toString(true) << endl;

            t1 = us();
            {
                db_t forked;
                db.fork(forked);
                t2 = us();
            }
            t3 = us();

//            cout << "db(simple):\n" << db.toString(true) << endl;

            cout << "fork " << c << " rows. cost: " << t2 - t1 << "us" << endl;
            cout << "destroy forked db. cost: " << t3 - t2 << "us" << endl;
            beforeDestory = us();
        }
        afterDestroy = us();
        cout << "destroy db. cost: " << afterDestroy - beforeDestory << "us" << endl;
        cout << "====================================" << endl;
    }
}

// ????????????, ??????fork
TEST(shadowdb_bench, fork_multi_large)
{
    db_t db;
    initDB(db, 100 * 1000, true, 10);

    int64_t t1 = us();
    db_t forked;
    db.fork(forked);
    int64_t t2 = us();
    EXPECT_TRUE(t2 - t1 < 10 * 1000);   // ??????10ms
    cout << "fork 10 levels, 100 * 1000 rows db. cost:" << t2 - t1 << "us" << endl;
}

// ?????????????????????
TEST(shadowdb_bench, index_yes_or_no)
{
    vector<int> levels = {
        1,
        2,
        3,
        5,
        10,
        20
    };

    int64_t t1, t2, rows;
    for (int indexY = 0; indexY < 2; ++indexY) {
        bool useIndex = !!indexY;
        for (int lv : levels) {
            db_t db;
            initDB(db, 100 * 1000, useIndex, lv);

            {
                ::shadow::Debugger dbg;
                std::vector<ProcessInfo const*> r = db.selectVector(
                        Cond(&ProcessInfo::id) >= Id{0, 1000}
                        && Cond(&ProcessInfo::id) < Id{0, 1010}
                        , &dbg);
                EXPECT_EQ(r.size(), 10);
                if (!useIndex) {
                    EXPECT_EQ(dbg.queryTrace.getScanRows(), db.size());
                }
            }

            t1 = us();
            for (int i = 0; i < 10; ++i) {
                std::vector<ProcessInfo const*> r = db.selectVector(
                        Cond(&ProcessInfo::id) >= Id{0, 1000}
                        && Cond(&ProcessInfo::id) < Id{0, 1010});
                rows += r.size();
            }
            t2 = us();
            cout << "[Index=" << useIndex << "]fork.level=" << lv
                << " select by scan 100 * 1000 rows return 10 rows. cost:"
                << (t2 - t1) / 10 << "us" << endl;
            
            if (!useIndex) {
                t1 = us();
                for (int i = 0; i < 10; ++i) {
                    db.foreach([&](string const&, ProcessInfo const& pi){
                            if (pi.id >= Id{0, 1000} && pi.id < Id{0, 1010})
                            ++rows;
                            return true;
                            });
                }
                t2 = us();
                cout << "fork.level=" << lv
                    << " foreach 100 * 1000 rows return 10 rows. cost:"
                    << (t2 - t1) / 10 << "us" << endl;
            }
        }
    }

    cout << "----------------------------------" << endl;
    cout << "ignore optimized output:" << rows << endl;
}

// ??????????????????
TEST(shadowdb_bench, mainkey)
{
    vector<int> levels = {
        1,
        2,
        3,
        5,
        10,
        20
    };

    int64_t t1, t2, rows = 0;
    for (int lv : levels) {
        db_t db;
        initDB(db, 100 * 1000, false, lv);

        {
            ProcessInfo pi;
            EXPECT_TRUE(db.get(std::to_string(1000), pi));
            EXPECT_TRUE(db.get(std::to_string(1000)));
        }

        t1 = ns();
        for (int i = 0; i < 1000; ++i) {
            ProcessInfo pi;
            rows += (int)db.get(std::to_string(1000), pi);
        }
        t2 = ns();
        cout << "fork.level=" << lv
            << " mainkey select in 100 * 1000 rows. cost:"
            << (t2 - t1) / 1000 << " ns" << endl;
    }

    cout << "----------------------------------" << endl;
    cout << "ignore optimized output:" << rows << endl;
}

