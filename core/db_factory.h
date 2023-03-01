//
//  db_factory.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_FACTORY_H_
#define YCSB_C_DB_FACTORY_H_

#include <map>
#include <string>

#include "db.h"
#include "measurements.h"
#include "properties.h"

namespace ycsbc {

class DBFactory {
   public:
    using DBCreator = DB *(*)(utils::Properties *props);
    static bool RegisterDB(std::string db_name, DBCreator db_creator);
    static DB *CreateDB(utils::Properties *props, Measurements *measurements);

   private:
    static std::map<std::string, DBCreator> &Registry();
};

}  // namespace ycsbc

#endif  // YCSB_C_DB_FACTORY_H_
