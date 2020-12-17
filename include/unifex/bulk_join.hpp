/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <unifex/receiver_concepts.hpp>
#include <unifex/sender_concepts.hpp>
#include <unifex/tag_invoke.hpp>
#include <unifex/execution_policy.hpp>
#include <unifex/get_execution_policy.hpp>
#include <unifex/bind_back.hpp>
#include <unifex/inplace_stop_token.hpp>
#include <unifex/get_bulk_controller.hpp>

#include <variant>
#include <optional>

#include <unifex/detail/prologue.hpp>

namespace unifex {

namespace _bulk_join {

template <typename... Errors>
using unique_decayed_error_types = concat_type_lists_unique_t<
    type_list<std::decay_t<Errors>>...>;

template <template <typename...> class Variant, typename... Senders>
using error_types =
    typename concat_type_lists_unique_t<
        sender_error_types_t<Senders, unique_decayed_error_types>...,
        type_list<std::exception_ptr>>::template apply<Variant>;

template <typename... Values>
using decayed_value_tuple = type_list<std::tuple<std::decay_t<Values>...>>;

template <typename Sender>
using value_variant_for_sender =
    typename sender_value_types_t<Sender, concat_type_lists_unique_t, decayed_value_tuple>
        ::template apply<std::variant>;

struct cancel_callback {
    inplace_stop_source& stopSource_;
    void operator()() noexcept { stopSource_.request_stop(); }
};

template <typename Source, typename Receiver>
struct _op {
    class type;
};

template <typename Source, typename Receiver>
using operation =
    typename _op<Source, Receiver>::type;

template<typename Source, typename Receiver>
struct _join_receiver {
    class type;
};

template<typename Source, typename Receiver>
using join_receiver =
    typename _join_receiver<Source, Receiver>::type;

template<typename Source, typename Receiver>
class _join_receiver<Source, Receiver>::type {
    using operation_type = operation<Source, Receiver>;
public:
    explicit type(operation_type * op) noexcept
    : op_(op)
    {}

    template(typename... Values)
        (requires all_void_type_lists_v<Values...>)
    void set_next(Values&&... values) & noexcept {
        op_->element_complete();
    }

    template(typename... Values)
        (requires receiver_of<Receiver, Values...>)
    void set_value(Values&&... values) noexcept(is_nothrow_receiver_of_v<Receiver, Values...>) {
        op_->value_.emplace(static_cast<Values &&>(values)...);
        op_->element_complete();
    }

    template(typename Error)
        (requires receiver<Receiver, Error>)
    void set_error(Error&& error) noexcept {
        op_->set_error(static_cast<Error&&>(error));
        op_->element_complete();
    }

    void set_done() noexcept {
        op_->set_done();
        op_->element_complete();
    }

    template(typename Error)
        (requires receiver<Receiver, Error>)
    void set_next_error(
        Error&& error) noexcept {
        op_->set_error(static_cast<Error&&>(error));
        op_->element_complete();
    }

    void set_next_done() noexcept {
        op_->set_done();
        op_->element_complete();
    }

    friend constexpr unifex::parallel_unsequenced_policy tag_invoke(
            tag_t<get_execution_policy>, [[maybe_unused]] const type& r) noexcept {
        return {};
    }

    template(typename CPO, typename Self, typename... Args)
        (requires
            is_receiver_query_cpo_v<CPO> AND
            same_as<Self, type>)
    friend auto tag_invoke(CPO cpo, const Self& self, Args&&... args)
        noexcept(is_nothrow_callable_v<CPO, const Receiver&, Args...>)
        -> callable_result_t<CPO, const Receiver&, Args...> {
        return cpo(self.get_receiver(), static_cast<Args&&>(args)...);
    }

    friend operation_type & tag_invoke(tag_t<unifex::get_bulk_controller>, const type& r) noexcept {
        return *r.op_;
    }

private:
    const Receiver & get_receiver() const noexcept {
        return op_->receiver_;
    }
    operation_type * op_;
};

template <typename Source, typename Receiver>
class _op<Source, Receiver>::type {
    using receiver_type = join_receiver<Source, Receiver>;

public:
    template <typename Receiver2>
    explicit type(Source && source, Receiver2 && receiver)
        : innerOp_(connect(
            static_cast<Source &&>(source),
            receiver_type{ this })),
        receiver_(static_cast<Receiver2 &&>(receiver)) {}

    void start() & noexcept {
         stopCallback_.emplace(get_stop_token(receiver_),
             cancel_callback{ stopSource_ });
        unifex::start(innerOp_);
    }

    void element_start(size_t count = 1) noexcept {
        refCount_.fetch_add(count, std::memory_order_relaxed);
    }

    void element_complete() noexcept {
        if (!refCount_.fetch_sub(1, std::memory_order_release)) {
            deliver_result();
        }
    }

    template(typename Error)(requires receiver<Receiver, Error>) void set_error(
        Error && error) noexcept {
        if (!doneOrError_.exchange(true, std::memory_order_relaxed)) {
            error_.emplace(std::in_place_type<std::decay_t<Error>>,
                static_cast<Error &&>(error));
            stopSource_.request_stop();
        }
    }

    void set_done() noexcept {
        if (!doneOrError_.exchange(true, std::memory_order_relaxed)) {
            stopSource_.request_stop();
        }
    }

    void deliver_result() noexcept {
        stopCallback_.reset();

        if (get_stop_token(receiver_).stop_requested()) {
            unifex::set_done(std::move(receiver_));
        }
        else if (doneOrError_.load(std::memory_order_relaxed)) {
            if (error_.has_value()) {
                std::visit(
                    [this](auto&& error) {
                        unifex::set_error(std::move(receiver_), (decltype(error))error);
                    },
                    std::move(error_.value()));
            }
            else {
                unifex::set_done(std::move(receiver_));
            }
        }
        else {
            std::visit(
                [this](auto&& value) {
                    std::apply([this](auto &&... values) {
                        unifex::set_value(std::move(receiver_), values...);
                        }, std::move(value));
                },
                std::move(value_.value()));
        }
    }

private:
    friend class _join_receiver<Source, Receiver>::type;

    connect_result_t<Source, receiver_type> innerOp_;
    Receiver receiver_;
    std::optional<value_variant_for_sender<remove_cvref_t<Source>>> value_;
    std::optional<error_types<std::variant, remove_cvref_t<Source>>> error_;
    inplace_stop_source stopSource_;
    std::optional<typename stop_token_type_t<Receiver>::template callback_type<
        cancel_callback>> stopCallback_;
    std::atomic_size_t refCount_{ 0 };
    std::atomic_bool doneOrError_{ false };
};

template<typename Source>
struct _join_sender {
    class type;
};

template<typename Source>
using join_sender = typename _join_sender<Source>::type;

template<typename Source>
class _join_sender<Source>::type {
public:
    template<template<typename...> class Variant, template<typename...> class Tuple>
    using value_types = sender_value_types_t<Source, Variant, Tuple>;

    template<template<typename...> class Variant>
    using error_types = sender_error_types_t<Source, Variant>;

    static constexpr bool sends_done = sender_traits<Source>::sends_done;

    template<typename Source2>
    explicit type(Source2&& s)
        noexcept(std::is_nothrow_constructible_v<Source, Source2>)
    : source_((Source2&&)s)
    {}

    template(typename Self, typename Receiver)
        (requires
            same_as<remove_cvref_t<Self>, type> AND
            sender_to<member_t<Self, Source>, join_receiver<remove_cvref_t<Source>, remove_cvref_t<Receiver>>>)
    friend auto tag_invoke(tag_t<unifex::connect>, Self&& self, Receiver&& r)
        noexcept(
            std::is_nothrow_constructible_v<remove_cvref_t<Receiver>> &&
            is_nothrow_connectable_v<member_t<Self, Source>, join_receiver<remove_cvref_t<Source>, remove_cvref_t<Receiver>>>)
    {
        return operation<Source, Receiver>{
            static_cast<Source&&>(static_cast<Self&&>(self).source_),
            static_cast<Receiver&&>(r)};
    }

private:
    Source source_;
};

struct _fn {
    template(typename Source)
        (requires
            typed_bulk_sender<Source> &&
            tag_invocable<_fn, Source>)
    auto operator()(Source&& source) const
        noexcept(is_nothrow_tag_invocable_v<_fn, Source>)
        -> tag_invoke_result_t<_fn, Source> {
        return tag_invoke(_fn{}, (Source&&)source);
    }

    template(typename Source)
        (requires
            typed_bulk_sender<Source> &&
            (!tag_invocable<_fn, Source>))
    auto operator()(Source&& source) const
        noexcept(std::is_nothrow_constructible_v<remove_cvref_t<Source>, Source>)
        -> join_sender<remove_cvref_t<Source>> {
        return join_sender<remove_cvref_t<Source>>{
            (Source&&)source};
    }
    constexpr auto operator()() const
        noexcept(is_nothrow_callable_v<
          tag_t<bind_back>, _fn>)
        -> bind_back_result_t<_fn> {
      return bind_back(*this);
    }
};

} // namespace _bulk_join

inline constexpr _bulk_join::_fn bulk_join{};

} // namespace unifex

#include <unifex/detail/epilogue.hpp>
