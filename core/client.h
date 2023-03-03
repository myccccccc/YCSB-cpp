//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <butil/time.h>

#include <string>

#include "core_workload.h"
#include "countdown_latch.h"
#include "db.h"
#include "utils.h"

namespace ycsbc {

inline int ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl,
                        const int num_ops, bool is_loading, bool init_db,
                        bool cleanup_db, CountDownLatch *latch) {
    if (init_db) {
        db->Init();
    }

    int ops = 0;
    if (is_loading) {
        for (int i = 0; i < num_ops;) {
            int batch_count = 0;
            if (i + wl->insert_batch_count() > num_ops) {
                batch_count = num_ops - i;
            } else {
                batch_count = wl->insert_batch_count();
            }

            wl->DoBatchInsert(batch_count, *db);
            ops += batch_count;
            i += batch_count;
        }
    } else {
        if (wl->bench_seconds()) {
            auto end_time = butil::gettimeofday_s() + wl->bench_seconds();
            while (butil::gettimeofday_s() < end_time) {
                if (wl->DoTx(*db)) {
                    ops++;
                }
            }
        } else {
            for (int i = 0; i < num_ops; ++i) {
                if (wl->DoTx(*db)) {
                    ops++;
                }
            }
        }
    }

    if (cleanup_db) {
        db->Cleanup();
    }

    latch->CountDown();
    return ops;
}

}  // namespace ycsbc

#endif  // YCSB_C_CLIENT_H_
