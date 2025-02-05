// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/config.hpp"

#include <condition_variable>
#include <limits>
#include <memory>
#include <thread>

#include "caf/detail/set_thread_name.hpp"
#include "caf/detail/thread_safe_actor_clock.hpp"
#include "caf/scheduler/abstract_coordinator.hpp"
#include "caf/scheduler/worker.hpp"

namespace caf::scheduler {

/// Policy-based implementation of the abstract coordinator base class.
template <class Policy>
class coordinator : public abstract_coordinator {
public:
  using super = abstract_coordinator;

  using policy_data = typename Policy::coordinator_data;

  coordinator(actor_system& sys) : super(sys), data_(this) {
    // nop
  }

  using worker_type = worker<Policy>;

  worker_type* worker_by_id(size_t x) {
    return workers_[x].get();
  }

  policy_data& data() {
    return data_;
  }

  static actor_system::module* make(actor_system& sys, detail::type_list<>) {
    return new coordinator(sys);
  }

protected:
  void start() override {
    // Create initial state for all workers.
    typename worker_type::policy_data init{this};
    // Prepare workers vector.
    auto num = num_workers();
    workers_.reserve(num);
    // Create worker instances.
    for (size_t i = 0; i < num; ++i)
      workers_.emplace_back(
        std::make_unique<worker_type>(i, this, init, max_throughput_));
    // Start all workers.
    for (auto& w : workers_)
      w->start();
    // Run remaining startup code.
    clock_.start_dispatch_loop(system());
    super::start();
  }

  void stop() override {
    // Shutdown workers.
    class shutdown_helper : public resumable, public ref_counted {
    public:
      resumable::resume_result resume(execution_unit* ptr, size_t) override {
        CAF_ASSERT(ptr != nullptr);
        std::unique_lock<std::mutex> guard(mtx);
        last_worker = ptr;
        cv.notify_all();
        return resumable::shutdown_execution_unit;
      }
      void intrusive_ptr_add_ref_impl() override {
        intrusive_ptr_add_ref(this);
      }

      void intrusive_ptr_release_impl() override {
        intrusive_ptr_release(this);
      }
      shutdown_helper() : last_worker(nullptr) {
        // nop
      }
      std::mutex mtx;
      std::condition_variable cv;
      execution_unit* last_worker;
    };
    // Use a set to keep track of remaining workers.
    shutdown_helper sh;
    std::set<worker_type*> alive_workers;
    auto num = num_workers();
    for (size_t i = 0; i < num; ++i) {
      alive_workers.insert(worker_by_id(i));
      sh.ref(); // Make sure reference count is high enough.
    }
    while (!alive_workers.empty()) {
      (*alive_workers.begin())->external_enqueue(&sh);
      // Since jobs can be stolen, we cannot assume that we have actually shut
      // down the worker we've enqueued sh to.
      { // lifetime scope of guard
        std::unique_lock<std::mutex> guard(sh.mtx);
        sh.cv.wait(guard, [&] { return sh.last_worker != nullptr; });
      }
      alive_workers.erase(static_cast<worker_type*>(sh.last_worker));
      sh.last_worker = nullptr;
    }
    // Shutdown utility actors.
    stop_actors();
    // Wait until all workers are done.
    for (auto& w : workers_) {
      w->get_thread().join();
    }
    // Run cleanup code for each resumable.
    auto f = &abstract_coordinator::cleanup_and_release;
    for (auto& w : workers_)
      policy_.foreach_resumable(w.get(), f);
    policy_.foreach_central_resumable(this, f);
    // Stop timer thread.
    clock_.stop_dispatch_loop();
  }

  void enqueue(resumable* ptr) override {
    policy_.central_enqueue(this, ptr);
  }

  detail::thread_safe_actor_clock& clock() noexcept override {
    return clock_;
  }

private:
  /// System-wide clock.
  detail::thread_safe_actor_clock clock_;

  /// Set of workers.
  std::vector<std::unique_ptr<worker_type>> workers_;

  /// Policy-specific data.
  policy_data data_;

  /// The policy object.
  Policy policy_;

  /// Thread for managing timeouts and delayed messages.
  std::thread timer_;
};

} // namespace caf::scheduler
