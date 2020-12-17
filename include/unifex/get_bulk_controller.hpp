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

#include <unifex/tag_invoke.hpp>
#include <unifex/type_traits.hpp>

#include <unifex/detail/prologue.hpp>

namespace unifex {

struct no_controller {
    static constexpr void element_start([[maybe_unused]] size_t count) noexcept { }
};

namespace _get_bulk_controller {
inline const struct _fn {
    template <typename T>
    constexpr auto operator()(const T&) const noexcept
        -> std::enable_if_t<!is_tag_invocable_v<_fn, const T&>, no_controller> {
        return {};
    }

    template <typename T>
    constexpr auto operator()(const T& object) const noexcept
        -> tag_invoke_result_t<_fn, const T&> {
      static_assert(
          is_nothrow_tag_invocable_v<_fn, const T&>,
          "get_bulk_controller() customisations must be declared noexcept");
      return tag_invoke(_fn{}, object);
    }
} get_bulk_controller{};
} // namespace _get_bulk_controller

using _get_bulk_controller::get_bulk_controller;

template <typename T>
using get_bulk_controller_result_t =
    callable_result_t<decltype(get_bulk_controller), T>;

template <typename Receiver>
using bulk_controller_type_t =
    remove_cvref_t<get_bulk_controller_result_t<Receiver>>;

} // namespace unifex

#include <unifex/detail/epilogue.hpp>
