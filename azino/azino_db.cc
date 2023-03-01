#include "azino_db.h"

#include "core/db_factory.h"

namespace {
const std::string TXPLANNER_ADDR = "azino.txplanner_addr";
const std::string TXPLANNER_ADDR_DEFAULT = "0.0.0.0:8001";

const std::string WRITE_TYPE = "azino.write_type";
const std::string WRITE_TYPE_DEFAULT = "auto";
}  // namespace

namespace ycsbc {

Azino::Azino() : tx_(nullptr) {}

Azino::~Azino() {}

void Azino::InitOptions() {
    const utils::Properties &props = *props_;

    opt_.txplanner_addr =
        props.GetProperty(TXPLANNER_ADDR, TXPLANNER_ADDR_DEFAULT);

    auto write_type = props.GetProperty(WRITE_TYPE, WRITE_TYPE_DEFAULT);
    if (write_type == "auto") {
        wopt_.type = azino::WriteType::kAutomatic;
    } else if (write_type == "pess") {
        wopt_.type = azino::WriteType::kPessimistic;
    } else if (write_type == "opti") {
        wopt_.type = azino::WriteType::kOptimistic;
    } else {
        throw utils::Exception(std::string("Invalid azino write type: ") +
                               write_type);
    }
}

void Azino::Init() {
    InitOptions();
    tx_ = new azino::Transaction(opt_);
}

void Azino::Cleanup() { delete tx_; }

DB::Status Azino::Read(const std::string &table, const std::string &key,
                       const std::vector<std::string> *fields,
                       std::vector<Field> &result) {
    std::string data;
    auto sts = tx_->Get(ropt_, key, data);
    if (sts.IsNotFound()) {
        return kNotFound;
    } else if (!sts.IsOk()) {
        throw utils::Exception(std::string("Azino Get: ") + sts.ToString());
    }
    if (fields != nullptr) {
        DeserializeRowFilter(&result, data, *fields);
    } else {
        DeserializeRow(&result, data);
    }
    return kOK;
}

DB::Status Azino::Scan(const std::string &table, const std::string &key,
                       int len, const std::vector<std::string> *fields,
                       std::vector<std::vector<Field>> &result) {
    return DB::kNotImplemented;
}

DB::Status Azino::Upsert(const std::string &table, const std::string &key,
                         std::vector<Field> &values) {
    std::string data;
    SerializeRow(values, &data);
    auto sts = tx_->Put(wopt_, key, data);
    return sts.IsOk() ? kOK : kError;
}

DB::Status Azino::Update(const std::string &table, const std::string &key,
                         std::vector<Field> &values) {
    return Upsert(table, key, values);
}

DB::Status Azino::Insert(const std::string &table, const std::string &key,
                         std::vector<Field> &values) {
    return Upsert(table, key, values);
}

DB::Status Azino::Delete(const std::string &table, const std::string &key) {
    std::string data;
    auto sts = tx_->Delete(wopt_, key);
    return sts.IsOk() ? kOK : kError;
}

void Azino::SerializeRow(const std::vector<Field> &values, std::string *data) {
    for (const Field &field : values) {
        uint32_t len = field.name.size();
        data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
        data->append(field.name.data(), field.name.size());
        len = field.value.size();
        data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
        data->append(field.value.data(), field.value.size());
    }
}

void Azino::DeserializeRowFilter(std::vector<Field> *values,
                                 const std::string &data,
                                 const std::vector<std::string> &fields) {
    const char *p = data.data();
    const char *lim = p + data.size();

    std::vector<std::string>::const_iterator filter_iter = fields.begin();
    while (p != lim && filter_iter != fields.end()) {
        assert(p < lim);
        uint32_t len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string field(p, static_cast<const size_t>(len));
        p += len;
        len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string value(p, static_cast<const size_t>(len));
        p += len;
        if (*filter_iter == field) {
            values->push_back({field, value});
            filter_iter++;
        }
    }
}

void Azino::DeserializeRow(std::vector<Field> *values,
                           const std::string &data) {
    const char *p = data.data();
    const char *lim = p + data.size();
    while (p != lim) {
        assert(p < lim);
        uint32_t len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string field(p, static_cast<const size_t>(len));
        p += len;
        len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string value(p, static_cast<const size_t>(len));
        p += len;
        values->push_back({field, value});
    }
}

DB::Status Azino::Begin() {
    tx_->Reset();
    auto sts = tx_->Begin();
    if (!sts.IsOk()) {
        throw utils::Exception(std::string("Azino Begin: ") + sts.ToString());
    }
    return kOK;
}

DB::Status Azino::Commit() {
    auto sts = tx_->Commit();
    if (!sts.IsOk()) {
        return kError;
    } else {
        return kOK;
    }
}

DB::Status Azino::Abort() {
    auto sts = tx_->Abort();
    if (!sts.IsOk()) {
        throw utils::Exception(std::string("Azino Abort: ") + sts.ToString());
    }
    return kOK;
}

DB *NewAzino() { return new Azino; }

const bool registered = DBFactory::RegisterDB("azino", NewAzino);
}  // namespace ycsbc