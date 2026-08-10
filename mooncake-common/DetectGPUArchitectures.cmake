# Match DeepEP-Sugon's build.sh detect_offload_arch selection policy.
function(_enumerate_gpu_architectures executable output_variable)
  set(_architectures "")
  if(executable)
    execute_process(
      COMMAND "${executable}"
      RESULT_VARIABLE _result
      OUTPUT_VARIABLE _output
      ERROR_QUIET)
    if(_result EQUAL 0)
      string(REPLACE "\r\n" ";" _agent_lines "${_output}")
      string(REPLACE "\n" ";" _agent_lines "${_agent_lines}")
      foreach(_agent IN LISTS _agent_lines)
        if(_agent MATCHES "^gfx[0-9]+")
          list(APPEND _architectures "${CMAKE_MATCH_0}")
        endif()
      endforeach()
      list(SORT _architectures ORDER DESCENDING)
      list(REMOVE_DUPLICATES _architectures)
    endif()
  endif()
  set(${output_variable} "${_architectures}" PARENT_SCOPE)
endfunction()

function(detect_gpu_architectures output_variable)
  set(_current_arch "")
  find_program(_rocminfo rocminfo)
  if(_rocminfo)
    execute_process(
      COMMAND "${_rocminfo}"
      RESULT_VARIABLE _result
      OUTPUT_VARIABLE _output
      ERROR_QUIET)
    if(_result EQUAL 0)
      string(REGEX MATCH "Name:[^\r\n]*gfx[0-9]+" _current_line "${_output}")
      string(REGEX MATCH "gfx[0-9]+" _current_arch "${_current_line}")
    endif()
  endif()

  find_program(_rocm_agent_enumerator rocm_agent_enumerator)
  _enumerate_gpu_architectures(
    "${_rocm_agent_enumerator}" _supported_architectures)

  # Without rocminfo, DeepEP falls back to the greatest enumerated target.
  if(NOT _current_arch)
    if(_supported_architectures)
      list(GET _supported_architectures 0 _fallback_arch)
      set(${output_variable} "${_fallback_arch}" PARENT_SCOPE)
    else()
      set(${output_variable} "" PARENT_SCOPE)
    endif()
    return()
  endif()

  # Otherwise select the two greatest enumerated targets and append the
  # current hardware target when it is not already among those two.
  if(_supported_architectures)
    list(LENGTH _supported_architectures _architecture_count)
    if(_architecture_count GREATER 2)
      set(_architecture_count 2)
    endif()
    list(SUBLIST _supported_architectures 0 ${_architecture_count}
         _gpu_architectures)
    list(FIND _gpu_architectures "${_current_arch}" _current_arch_index)
    if(_current_arch_index EQUAL -1)
      list(APPEND _gpu_architectures "${_current_arch}")
    endif()
    set(${output_variable} "${_gpu_architectures}" PARENT_SCOPE)
  else()
    # DeepEP retries the enumerator before its final greatest-target fallback.
    _enumerate_gpu_architectures(
      "${_rocm_agent_enumerator}" _fallback_architectures)
    if(_fallback_architectures)
      list(GET _fallback_architectures 0 _fallback_arch)
      set(${output_variable} "${_fallback_arch}" PARENT_SCOPE)
    else()
      set(${output_variable} "" PARENT_SCOPE)
    endif()
  endif()
endfunction()
