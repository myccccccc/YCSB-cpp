#ifndef YCSB_C_AZINO_DB_H_
#define YCSB_C_AZINO_DB_H_

#include <azino/client.h>

#include <iostream>
#include <mutex>
#include <string>

#include "core/db.h"
#include "core/properties.h"

namespace ycsbc {

class Azino : public DB {
   public:
    Azino();
    ~Azino();

    void Init();
    void Cleanup();

    virtual Status Begin();
    virtual Status Commit();
    virtual Status Abort();

    Status Read(const std::string &table, const std::string &key,
                const std::vector<std::string> *fields,
                std::vector<Field> &result);

    Status Scan(const std::string &table, const std::string &key, int len,
                const std::vector<std::string> *fields,
                std::vector<std::vector<Field>> &result);

    Status Update(const std::string &table, const std::string &key,
                  std::vector<Field> &values);

    Status Insert(const std::string &table, const std::string &key,
                  std::vector<Field> &values);

    Status Delete(const std::string &table, const std::string &key);

   private:
    void SerializeRow(const std::vector<Field> &values, std::string *data);
    void DeserializeRow(std::vector<Field> *values, const std::string &data);

    azino::Transaction *tx;
};

DB *NewAzino();

}  // namespace ycsbc

#endif  // YCSB_C_AZINO_DB_H_
