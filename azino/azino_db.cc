#include "azino_db.h"

#include "core/db_factory.h"

namespace {
const std::string TXPLANNER_ADDR = "azino.txplanner_addr";
const std::string TXPLANNER_ADDR_DEFAULT = "0.0.0.0:8001";
}  // namespace

namespace ycsbc {

Azino::Azino() : tx(nullptr) {}

Azino::~Azino() {}

void Azino::Init() {
    const utils::Properties &props = *props_;
    const std::string &txplanner_addr =
        props.GetProperty(TXPLANNER_ADDR, TXPLANNER_ADDR_DEFAULT);

    azino::Options options;
    tx = new azino::Transaction(options, txplanner_addr);
}

void Azino::Cleanup() { delete tx; }

DB::Status Azino::Read(const std::string &table, const std::string &key,
                       const std::vector<std::string> *fields,
                       std::vector<Field> &result) {
    return DB::kNotImplemented;
}

DB::Status Azino::Scan(const std::string &table, const std::string &key,
                       int len, const std::vector<std::string> *fields,
                       std::vector<std::vector<Field>> &result) {
    return DB::kNotImplemented;
}

DB::Status Azino::Update(const std::string &table, const std::string &key,
                         std::vector<Field> &values) {
    return DB::kNotImplemented;
}

DB::Status Azino::Insert(const std::string &table, const std::string &key,
                         std::vector<Field> &values) {
    return DB::kNotImplemented;
}

DB::Status Azino::Delete(const std::string &table, const std::string &key) {
    return DB::kNotImplemented;
}

void Azino::SerializeRow(const std::vector<Field> &values, std::string *data) {}

void Azino::DeserializeRow(std::vector<Field> *values,
                           const std::string &data) {}

DB::Status Azino::Begin() {
    tx->Reset();
    auto sts = tx->Begin();
    if (!sts.IsOk()) {
        throw utils::Exception(std::string("Azino Begin: ") + sts.ToString());
    }
    return kOK;
}

DB::Status Azino::Commit() {
    auto sts = tx->Commit();
    if (!sts.IsOk()) {
        return kError;
    } else {
        return kOK;
    }
}

DB::Status Azino::Abort() {
    auto sts = tx->Abort();
    if (!sts.IsOk()) {
        throw utils::Exception(std::string("Azino Abort: ") + sts.ToString());
    }
    return kOK;
}

DB *NewAzino() { return new Azino; }

const bool registered = DBFactory::RegisterDB("azino", NewAzino);
}  // namespace ycsbc