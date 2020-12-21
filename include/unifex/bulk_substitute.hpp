/*
 * Copyright 2019-present Facebook, Inc.
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

#include <unifex/config.hpp>
#include <unifex/get_bulk_controller.hpp>
#include <unifex/let_with.hpp>
#include <unifex/receiver_concepts.hpp>
#include <unifex/scheduler_concepts.hpp>
#include <unifex/sender_concepts.hpp>
#include <unifex/type_list.hpp>

#include <unifex/detail/prologue.hpp>

namespace unifex {

namespace _bulk_substitute {

template <typename Source, typename Receiver>
struct _element_receiver {
    struct type;
};

template <typename Source, typename Receiver>
using element_receiver =
    typename _element_receiver<Source, Receiver>::type;

template <typename Source, typename Receiver>
struct _element_receiver<Source, Receiver>::type {
    using element_receiver = type;
    using bulk_controller_t = get_bulk_controller_result_t<Receiver>;
    
    Receiver& receiver_;
    UNIFEX_NO_UNIQUE_ADDRESS bulk_controller_t bulk_controller_ =
        get_bulk_controller(receiver_);
    
    template(typename... Values)
        (requires is_next_receiver_v<Receiver, Values...> AND typed_bulk_sender<Source>)
    void set_next(Values&&... values) & noexcept {
        unifex::set_next(receiver_, static_cast<Values&&>(values)...);
    }
    
    template(typename T = void)
        (requires typed_bulk_sender<Source>)
    void set_value() noexcept
        /*requires typed_bulk_sender<Source>*/ {
        if constexpr (!std::is_same_v<bulk_controller_t, no_controller>) {
            bulk_controller_.element_complete();
        }
    }
    
    template(typename... Values)
        (requires is_next_receiver_v<Receiver, Values...> AND (!typed_bulk_sender<Source>))
    void set_value(Values&&... values)
        noexcept(is_nothrow_receiver_of_v<Receiver, Values...>) {
        unifex::set_next(receiver_, static_cast<Values&&>(values)...);
    }
    
    template <typename Error>
    void set_error(Error&& error) noexcept {
        unifex::set_next_error(receiver_, static_cast<Error&&>(error));
    }
    
    void set_done() && noexcept {
        unifex::set_next_done(receiver_);
    }
    
    template(typename Error)
        (requires typed_bulk_sender<Source>)
    void set_next_error(Error&& error) noexcept {
        unifex::set_next_error(receiver_, static_cast<Error&&>(error));
    }
    
    template(typename T = void)
        (requires typed_bulk_sender<Source>)
    void set_next_done() noexcept
        /*requires typed_bulk_sender<Source> */{
        unifex::set_next_done(receiver_);
    }
    
    template(typename CPO, typename Self, typename... Args)
        (requires
            is_receiver_query_cpo_v<CPO> AND
            same_as<Self, type>)
        friend auto tag_invoke(CPO cpo, const Self & self, Args&&... args)
        noexcept(is_nothrow_callable_v<CPO, const Receiver &, Args...>)
        -> callable_result_t<CPO, const Receiver &, Args...> {
        return cpo(self.receiver_, static_cast<Args&&>(args)...);
    }
    
    template <typename Func>
    friend void tag_invoke(tag_t<visit_continuations>, const element_receiver& r, Func&& func) {
        std::invoke(func, r.receiver_);
    }
};

template <typename SuccessorFactory, typename Receiver, typename Policy>
struct _bulk_substitute_receiver {
    class type;
};

template <typename SuccessorFactory, typename Receiver, typename Policy>
using bulk_substitute_receiver =
    typename _bulk_substitute_receiver<SuccessorFactory, Receiver, Policy>::type;

template <typename SuccessorFactory, typename Receiver, typename Policy>
class _bulk_substitute_receiver<SuccessorFactory, Receiver, Policy>::type {
public:
    template <typename SuccessorFactory2, typename Receiver2>
    explicit type(SuccessorFactory2&& successorFactory, Receiver2&& r, Policy policy)
        noexcept(std::is_nothrow_constructible_v<SuccessorFactory, SuccessorFactory2> &&
            std::is_nothrow_constructible_v<Receiver, Receiver2>)
        : successorFactory_(static_cast<SuccessorFactory2&&>(successorFactory))
        , receiver_(static_cast<Receiver2&&>(r))
        , policy_(static_cast<Policy &&>(policy)) {}
    
    template <typename... Values>
    void set_next(Values&&... values) & noexcept {
        using element_source_type =
            std::invoke_result_t<SuccessorFactory, std::decay_t<Values>&...>;
        UNIFEX_TRY {
            submit(
                let_with(
                    [... values = std::forward<Values>(values)]() {
                        return std::move(values...);
                    },
                    [&](auto&... value) {
                        return successorFactory_(value...);
                    }
                ),
                element_receiver<element_source_type, Receiver>{receiver_}
            );
        }
        UNIFEX_CATCH(...) {
            unifex::set_error(std::move(receiver_), std::current_exception());
        }
    }
    
    template(typename... Values)
        (requires receiver_of<Receiver, Values...>)
    void set_value(Values&&... values) &&
        noexcept(is_nothrow_receiver_of_v<Receiver, Values...>) {
        unifex::set_value(std::move(receiver_), static_cast<Values &&>(values)...);
    }
    
    template(typename Error)
        (requires receiver<Receiver, Error>)
    void set_error(Error&& error) && noexcept {
      unifex::set_error(std::move(receiver_), static_cast<Error&&>(error));
    }
    
    void set_done() && noexcept {
        unifex::set_done(std::move(receiver_));
    }
    
    template(typename Error)
        (requires receiver<Receiver, Error>)
    void set_next_error(Error&& error) & noexcept {
        unifex::set_next_error(receiver_, static_cast<Error&&>(error));
    }
    
    void set_next_done() & noexcept {
        unifex::set_next_done(receiver_);
    }

    friend auto tag_invoke(tag_t<get_execution_policy>, const type& r) noexcept {
        using receiver_policy = decltype(get_execution_policy(r.receiver_));
        constexpr bool allowUnsequenced =
          is_one_of_v<receiver_policy, unsequenced_policy, parallel_unsequenced_policy> &&
          is_one_of_v<Policy, unsequenced_policy, parallel_unsequenced_policy>;
        constexpr bool allowParallel =
          is_one_of_v<receiver_policy, parallel_policy, parallel_unsequenced_policy> &&
          is_one_of_v<Policy, parallel_policy, parallel_unsequenced_policy>;

        if constexpr (allowUnsequenced && allowParallel) {
            return unifex::par_unseq;
        } else if constexpr (allowUnsequenced) {
            return unifex::unseq;
        } else if constexpr (allowParallel) {
            return unifex::par;
        } else {
            return unifex::seq;
        }
    }
    
    template(typename CPO, typename Self, typename... Args)
        (requires
            is_receiver_query_cpo_v<CPO> AND
            same_as<Self, type>)
        friend auto tag_invoke(CPO cpo, const Self& self, Args&&... args)
        noexcept(is_nothrow_callable_v<CPO, const Receiver&, Args...>)
        -> callable_result_t<CPO, const Receiver&, Args...> {
        return cpo(self.receiver_, static_cast<Args&&>(args)...);
    }
    
private:
    UNIFEX_NO_UNIQUE_ADDRESS SuccessorFactory successorFactory_;
    UNIFEX_NO_UNIQUE_ADDRESS Receiver receiver_;
    UNIFEX_NO_UNIQUE_ADDRESS Policy policy_;
};

template <typename Sender>
struct sends_done_impl :
    std::bool_constant<sender_traits<Sender>::sends_done> {};

template <typename... Successors>
using any_sends_done =
    std::disjunction<sends_done_impl<Successors>...>;

template <
    typename Sender,
    template <typename...> class Variant,
    template <typename...> class Tuple,
    typename Enable = void>
struct successor_next_types_impl;

template <
    typename Sender,
    template <typename...> class Variant,
    template <typename...> class Tuple>
struct successor_next_types_impl<Sender, Variant, Tuple, std::enable_if_t<typed_bulk_sender<Sender>>> {
    using type =
        typename sender_traits<Sender>::template next_types<Variant, Tuple>;
};

template <
    typename Sender,
    template <typename...> class Variant,
    template <typename...> class Tuple>
struct successor_next_types_impl<Sender, Variant, Tuple, std::enable_if_t<!typed_bulk_sender<Sender>>> {
    using type =
        typename sender_traits<Sender>::template value_types<Variant, Tuple>;
};

template <
    typename Sender,
    template <typename...> class Variant,
    template <typename...> class Tuple>
using successor_next_types_t =
    typename successor_next_types_impl<Sender, Variant, Tuple>::type;

template <typename Source, typename SuccessorFactory, typename Policy>
struct _substitute_sender {
    class type;
};

template <typename Source, typename SuccessorFactory, typename Policy>
using substitute_sender =
    typename _substitute_sender<Source, SuccessorFactory, Policy>::type;

template <typename Source, typename SuccessorFactory, typename Policy>
class _substitute_sender<Source, SuccessorFactory, Policy>::type {
    using substitute_sender = type;

    template <typename... Values>
    using successor_type =
        std::invoke_result_t<SuccessorFactory, std::decay_t<Values>&...>;
    
    template <template <typename...> class List>
    using successor_types = sender_next_types_t<Source, List, successor_type>;

    template <
        template <typename...> class Variant,
        template <typename...> class Tuple>
    struct next_types_impl {
        template <typename... Senders>
        using apply =
            typename concat_type_lists_unique_t<
                successor_next_types_t<Senders, type_list, Tuple>...>::template apply<Variant>;
    };
    
    template <template <typename...> class Variant>
    struct error_types_impl {
        template <typename... Senders>
        using apply =
            typename concat_type_lists_unique_t<
                sender_error_types_t<Senders, type_list>...,
                type_list<std::exception_ptr>>::template apply<Variant>;
    };
    
public:
    template <
        template <typename...> class Variant,
        template <typename...> class Tuple>
    using next_types =
        successor_types<next_types_impl<Variant, Tuple>::template apply>;
    
    template <
        template <typename...> class Variant,
        template <typename...> class Tuple>
    using value_types =
        sender_value_types_t<Source, Variant, Tuple>;
    
    template <template <typename...> class Variant>
    using error_types =
        successor_types<error_types_impl<Variant>::template apply>;
    
    static constexpr bool sends_done = sender_traits<Source>::sends_done ||
        successor_types<any_sends_done>::value;
    
    template <typename Source2, typename SuccessorFactory2>
    explicit type(Source2&& source, SuccessorFactory2&& successorFactory, Policy policy)
        noexcept(std::is_nothrow_constructible_v<Source,Source2> &&
            std::is_nothrow_constructible_v<SuccessorFactory, SuccessorFactory2>)
        : source_(static_cast<Source2&&>(source))
        , successorFactory_(static_cast<SuccessorFactory2&&>(successorFactory))
        , policy_(static_cast<Policy&&>(policy)) {}
    
    template(typename Self, typename Receiver)
        (requires same_as<remove_cvref_t<Self>, type> AND
            constructible_from<SuccessorFactory, member_t<Self, SuccessorFactory>> AND
            receiver<Receiver> AND
            sender_to<member_t<Self, Source>, bulk_substitute_receiver<SuccessorFactory, remove_cvref_t<Receiver>, Policy>>)
    friend auto tag_invoke(tag_t<unifex::connect>, Self&& self, Receiver&& r)
        noexcept(std::is_nothrow_constructible_v<Source, member_t<Self, Source>> &&
            std::is_nothrow_constructible_v<SuccessorFactory, member_t<Self, SuccessorFactory>> &&
            std::is_nothrow_constructible_v<remove_cvref_t<Receiver>, Receiver> &&
            std::is_nothrow_constructible_v<Policy, member_t<Self, Policy>>) {
        return unifex::connect(static_cast<Self&&>(self).source_,
            bulk_substitute_receiver<SuccessorFactory, remove_cvref_t<Receiver>, Policy>{
                static_cast<Self&&>(self).successorFactory_,
                static_cast<Receiver&&>(r),
                static_cast<Self&&>(self).policy_});
    }
    
    friend constexpr blocking_kind
    tag_invoke(tag_t<blocking>, const substitute_sender& sender) {
        return blocking(sender.source_) == blocking_kind::never
            ? blocking_kind::never
            : blocking_kind::maybe;
    }
    
private:
    UNIFEX_NO_UNIQUE_ADDRESS Source source_;
    UNIFEX_NO_UNIQUE_ADDRESS SuccessorFactory successorFactory_;
    UNIFEX_NO_UNIQUE_ADDRESS Policy policy_;
};

struct _fn {
    template(typename Source, typename SuccessorFactory, typename Policy = decltype(get_execution_policy(UNIFEX_DECLVAL(SuccessorFactory &))))
        (requires typed_bulk_sender<Source>)
    auto operator()(Source && source, SuccessorFactory && factory) const
        noexcept(is_nothrow_tag_invocable_v<_fn, Source, SuccessorFactory, Policy>)
        -> callable_result_t<_fn, Source, SuccessorFactory, Policy> {
        return operator()(
            static_cast<Source&&>(source),
            static_cast<SuccessorFactory&&>(factory),
            get_execution_policy(factory));
    }

    template(typename Source, typename SuccessorFactory, typename Policy)
        (requires typed_bulk_sender<Source> AND
            tag_invocable<_fn, Source, SuccessorFactory, Policy>)
    auto operator()(Source&& source, SuccessorFactory&& factory, Policy policy) const
        noexcept(is_nothrow_tag_invocable_v<_fn, Source, SuccessorFactory, Policy>)
        -> tag_invoke_result_t<_fn, Source, SuccessorFactory, Policy> {
        return tag_invoke(_fn{},
            static_cast<Source&&>(source),
            static_cast<SuccessorFactory&&>(factory),
            static_cast<Policy &&>(policy));
    }
    
    template(typename Source, typename SuccessorFactory, typename Policy)
        (requires typed_bulk_sender<Source> AND
            (!tag_invocable<_fn, Source, SuccessorFactory, Policy>))
    auto operator()(Source&& source, SuccessorFactory&& factory, Policy policy) const
        noexcept(std::is_nothrow_constructible_v<remove_cvref_t<Source>, Source> &&
            std::is_nothrow_constructible_v<remove_cvref_t<SuccessorFactory>, SuccessorFactory> &&
            std::is_nothrow_move_constructible_v<Policy>)
        -> substitute_sender<remove_cvref_t<Source>, remove_cvref_t<SuccessorFactory>, Policy> {
        return substitute_sender<remove_cvref_t<Source>, remove_cvref_t<SuccessorFactory>, Policy> {
            static_cast<Source&&>(source),
            static_cast<SuccessorFactory&&>(factory),
            static_cast<Policy &&>(policy)};
    }
    
    template <typename SuccessorFactory>
    constexpr auto operator()(SuccessorFactory&& factory) const
        noexcept(is_nothrow_callable_v<tag_t<bind_back>, _fn, SuccessorFactory>)
        -> bind_back_result_t<_fn, SuccessorFactory> {
        return bind_back(*this, static_cast<SuccessorFactory&&>(factory));
    }

    template <typename SuccessorFactory, typename Policy>
    constexpr auto operator()(SuccessorFactory && factory, Policy policy) const
        noexcept(is_nothrow_callable_v<tag_t<bind_back>, _fn, SuccessorFactory, Policy>)
        -> bind_back_result_t<_fn, SuccessorFactory, Policy> {
        return bind_back(*this, static_cast<SuccessorFactory &&>(factory), static_cast<Policy &&>(policy));
    }
};

}  // namespace _bulk_substitute

inline constexpr _bulk_substitute::_fn bulk_substitute{};

}  // namespace unifex

#include <unifex/detail/epilogue.hpp>
