#include "db.h"

namespace ycsbc {
    DB::Status DB::Begin() {
        return DB::kOK;
    }

    DB::Status DB::Commit() {
        return DB::kOK;
    }

    DB::Status DB::Abort() {
        return DB::kOK;
    }
}