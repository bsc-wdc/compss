#!/usr/bin/env python3

"""
THIS SCRIPT PARSES THE expected_events.txt FILE AND PERFORMS ALL INDICATED
CHECKS INTO THE TEST TRACE ACHIEVED IN THIS TEST.

INVOCATION:
    python3 events_checker.py expecte_events.txt
"""

import sys

COMMENT_LABEL = "#"
FAMILY_LABEL = "Family"
EVENT_LABEL = "event"
RANGE_LABEL = "range"


def parse_file(expected_events_file):
    """
    Reads the expected_events_file and builds the families and event_definitions
    data structures.

    - families structure:
      [ ( type, name ) ]
    - event_definitions structure:
      { type : { EVENT_LABEL : { event : appearances}
                 RANGE_LABEL : [ (min_event, max_event, appearances) ] }}

    :param expected_events_file: File that contains the rules to check.
    :return: Families and event definitions.
    """
    print("\n- Parsing expected events file: %s" % expected_events_file)
    with open(expected_events_file, "r") as fd:
        lines = fd.readlines()
    families = []
    event_definitions = {}
    for line in lines:
        line = line.strip()
        if line == "":
            # empty line: ignore
            pass
        elif line.startswith(COMMENT_LABEL):
            # It is a comment: ignore
            pass
        elif line.startswith(FAMILY_LABEL):
            # It is a family entry
            f_elements = line.split()
            f_type = int(f_elements[1])
            f_name = f_elements[2]
            families.append((f_type, f_name))
        else:
            # It is an event/range definition
            d_elements = line.split()
            d_type = int(d_elements[0])
            d_mode = d_elements[1]
            if d_mode == EVENT_LABEL:
                d_event = int(d_elements[2])
                d_appearances = d_elements[3]
                if d_appearances == "undefined":
                    d_appearances = -1
                elif "," in d_appearances:
                    d_appearances_values = d_appearances.split(",")
                    d_appearances = [int(d_app) for d_app in d_appearances_values]
                else:
                    d_appearances = int(d_appearances)
                if d_appearances > 0 or d_appearances == -1:
                    if d_type not in list(event_definitions.keys()):
                        # create new type
                        event_definitions[d_type] = {}
                    if EVENT_LABEL not in list(event_definitions[d_type].keys()):
                        # create event label
                        event_definitions[d_type][EVENT_LABEL] = {}
                    if d_event in list(event_definitions[d_type][EVENT_LABEL].keys()):
                        # redefined event
                        raise Exception(
                            "ERROR: Event defined twice: %s %s" % (d_type, d_event)
                        )
                    else:
                        # include event
                        event_definitions[d_type][EVENT_LABEL][d_event] = d_appearances
            if d_mode == RANGE_LABEL:
                d_min_event = int(d_elements[2])
                d_max_event = int(d_elements[3])
                d_appearances = d_elements[4]
                if d_appearances == "undefined":
                    d_appearances = -1
                elif "," in d_appearances:
                    d_appearances_values = d_appearances.split(",")
                    d_appearances = [int(d_app) for d_app in d_appearances_values]
                else:
                    d_appearances = int(d_appearances)
                if (
                    isinstance(d_appearances, int)
                    and (d_appearances > 0 or d_appearances == -1)
                ) or (isinstance(d_appearances, list)):
                    if d_type not in list(event_definitions.keys()):
                        # create new type
                        event_definitions[d_type] = {}
                    if RANGE_LABEL not in list(event_definitions[d_type].keys()):
                        # create range label
                        event_definitions[d_type][RANGE_LABEL] = []
                    else:
                        # redefined erange
                        raise Exception("ERROR: Event range defined twice: %s" % d_type)
                    # include event
                    event_definitions[d_type][RANGE_LABEL] = (
                        d_min_event,
                        d_max_event,
                        d_appearances,
                    )
            if d_mode != EVENT_LABEL and d_mode != RANGE_LABEL:
                raise Exception(
                    "Unsupported event mode: %s (supported are: event or range)"
                    % d_mode
                )
    print("\t- Rules:")
    print(event_definitions)
    return families, event_definitions


def __pairwise__(iterable):
    """Converts a list of elements in a list of pairs like:
    list -> (list[0], list[1]), (list[2], list[3]), (list[4], list[5]), ...

    :param iterable: Input list.
    :return: List of pairs of the given list elements.
    """
    a = iter(iterable)
    return list(zip(a, a))


def parse_trace(trace_file, families):
    """
    Reads the given trace_file and filters the event types by family.
    NOTE: Filters the lines that start with # (comments) and with 1: (states)
          Events start with 2:

    :param trace_file: Trace file to parse.
    :param families: Families to look for.
    :return: Trace events (list of pairs (type, event))
    """
    print("\n- Parsing trace file... %s" % trace_file)
    with open(trace_file, "r") as fd:
        trace = fd.readlines()
    parsed_trace = []
    for line in trace:
        if not line.startswith(COMMENT_LABEL) and not line.startswith("1:"):
            elements = line.split(":")
            event_elements = elements[6:]  # remove the headers
            # Parse the rest by pairs
            for event_type, event_number in __pairwise__(event_elements):
                parsed_trace.append(
                    (int(event_type.strip()), int(event_number.strip()))
                )
    print("\t- Filtering families... %s" % str(families))
    filtered_trace = []
    for line in parsed_trace:
        for family in families:
            if line[0] - family[0] >= 0 and line[0] - family[0] < 1000000:
                # belongs to the million
                filtered_trace.append(line)
    return filtered_trace


def check_families(trace_events, families, event_definitions):
    """
    Looks for event types found in the trace that are not specified in the
    event definitions.

    :param trace_events: Events found in the trace (filtered by family).
    :param families: Families to look for.
    :param event_definitions: Event definitions.
    :return: Message report list
    """
    print("\n- Checking families... %s" % families)
    report = []
    trace_event_types = []
    for line in trace_events:
        if line[0] not in trace_event_types:
            trace_event_types.append(line[0])
    trace_event_types.sort()
    print("\t- Found event types:")
    print(str(trace_event_types))
    event_types = list(event_definitions.keys())
    event_types = sorted(event_types)
    print("\t- Expected event types:")
    print(str(event_types))
    for event_type in trace_event_types:
        if event_type not in event_types:
            report.append(
                "ERROR: Unexpected event type %d found in check_families" % event_type
            )
    for event in event_types:
        if event not in trace_event_types:
            report.append(
                "ERROR: Missing event type %d found in check_families" % event
            )
    return report


def check_events(trace_events, event_definitions):
    """
    Checks if the events found in trace_events are defined in
    the event_definitions.

    :param trace_events: Events found in the trace (filtered by family).
    :param event_definitions: Event definitions.
    :return: Message report list
    """
    print("\n- Checking events...")
    report = []
    unique_events = __unique_trace_events__(trace_events)
    for event_type, rule in event_definitions.items():
        print("\t- Event type: %d Rule: %s" % (event_type, str(rule)))
        ok = True
        if event_type not in unique_events:
            report.append(
                "ERROR: Missing event type %d found in check_events" % event_type
            )
        else:
            unique_events_type = unique_events[event_type]
            unique_events_type.sort()
            if EVENT_LABEL in rule:
                expected_events = list(rule[EVENT_LABEL].keys())
                expected_events = sorted(expected_events)
                # Check that all expected events are found
                for ev in expected_events:
                    if ev not in unique_events_type:
                        report.append(
                            "ERROR: Missing event %d:%d found" % (event_type, ev)
                        )
                        ok = False
                # Check if there are events not defined in expected events
                if RANGE_LABEL not in rule:
                    for ev in unique_events_type:
                        if ev not in expected_events:
                            report.append(
                                "ERROR: Unexpected event %d:%d found" % (event_type, ev)
                            )
                            ok = False
            if RANGE_LABEL in rule:
                # TODO: Falta aqui comprobar que la suma de eventos equivale al 0
                expected_range = (rule[RANGE_LABEL][0], rule[RANGE_LABEL][1])
                unique_events_type.pop(0)  # remove 0
                for ev in unique_events_type:
                    min_ev = expected_range[0]
                    max_ev = expected_range[1]
                    if ev < min_ev or ev > max_ev:
                        report.append(
                            "ERROR: Event out of range of type %d found with value %d (Expected range: %d-%d)"
                            % (event_type, ev, min_ev, max_ev)
                        )
                        ok = False
                if EVENT_LABEL not in rule:
                    report.append(
                        "ERROR: 0 events undefined for range %d" % (event_type)
                    )
                    ok = False
        if not ok:
            print("\t\t- ERROR found")
    return report


def __unique_trace_events__(trace_events):
    """
    Finds unique trace events.

    :param trace_events: Events found in the trace (filtered by family).
    :return: Unique trace events
    """
    unique_events = {}
    for line in trace_events:
        if line[0] not in list(unique_events.keys()):
            unique_events[line[0]] = [line[1]]
        else:
            if line[1] not in unique_events[line[0]]:
                unique_events[line[0]].append(line[1])
    return unique_events


def check_rules(trace_events, event_definitions):
    """
    Checks if the events found in trace_events match with the defined in
    event definitions

    :param trace_events: Events found in the trace (filtered by family).
    :param event_definitions: Event definitions.
    :return: Message report list
    """
    print("\n- Checking that rules are applied...")
    report = []
    for event_type, rule in event_definitions.items():
        print("\t- Checking rule: %d %s" % (event_type, str(rule)))
        is_undefined = False
        undefined_appearances = None
        filtered_trace_events = __filter_event_type__(trace_events, event_type)
        if EVENT_LABEL in rule:
            event_ok = True
            accumulated_events = __accumulate_events__(
                filtered_trace_events, rule[EVENT_LABEL]
            )
            for event, appearances in accumulated_events.items():
                if rule[EVENT_LABEL][event] == -1:
                    is_undefined = True
                    undefined_appearances = appearances
                elif isinstance(rule[EVENT_LABEL][event], list):
                    if appearances not in rule[EVENT_LABEL][event]:
                        report.append(
                            "ERROR: Unexpected type %d event %d appearances found: %d (Expected %d)"
                            % (
                                event_type,
                                event,
                                appearances,
                                str(rule[EVENT_LABEL][event]),
                            )
                        )
                        event_ok = False
                elif appearances != rule[EVENT_LABEL][event]:
                    report.append(
                        "ERROR: Unexpected type %d event %d appearances found: %d (Expected %d)"
                        % (event_type, event, appearances, rule[EVENT_LABEL][event])
                    )
                    event_ok = False
                else:
                    pass  # ok
            if is_undefined:
                print(
                    "\t\t- UNDEFINED appearances (%s) %d"
                    % (EVENT_LABEL, undefined_appearances)
                )
            elif event_ok:
                print("\t\t- OK appearances (%s)" % EVENT_LABEL)
            else:
                print("\t\t- ERROR appearances (%s)" % (EVENT_LABEL))
            # TODO: Check amount of zeros - some of the runtime 0 events are emitted as 0 (constraints)
            # if RANGE_LABEL not in rule:
            #     expected_zeros = accumulated_events[0]
            #     amount_zeros = 0
            #     for event, appearances in accumulated_events.items():
            #         if event != 0:
            #             amount_zeros += appearances
            #     if expected_zeros != amount_zeros:
            #         report.append("ERROR: Unexpected amount of zeros in type %d found %d (Expected %d)" % (event_type, amount_zeros, expected_zeros))

        is_undefined_range = False
        undefined_appearances_range = None
        if RANGE_LABEL in rule:
            range_ok = True
            accumulated_range = __accumulate_range__(filtered_trace_events)
            expected_amount = rule[RANGE_LABEL][2]
            found_appearances = len(accumulated_range)
            if expected_amount == -1:
                is_undefined_range = True
                undefined_appearances_range = found_appearances
            elif isinstance(expected_amount, list):
                if found_appearances not in expected_amount:
                    report.append(
                        "ERROR: Unexpected event range of type %s found: %s (Expected %s)"
                        % (str(event_type), str(found_appearances), str(expected_amount))
                    )
                    range_ok = False
            elif found_appearances != expected_amount:
                report.append(
                    "ERROR: Unexpected event range of type %s found: %s (Expected %s)"
                    % (str(event_type), str(found_appearances), str(expected_amount))
                )
                range_ok = False
            if is_undefined_range:
                print(
                    "\t\t- UNDEFINED appearances (%s) %s"
                    % (RANGE_LABEL, str(undefined_appearances_range))
                )
            elif range_ok:
                print("\t\t- OK appearances (%s)" % RANGE_LABEL)
            else:
                print("\t\t- ERROR appearances (%s)" % (RANGE_LABEL))

        # if event 0 is undefined and range is undefined, check that they match
        if is_undefined and is_undefined_range:
            if undefined_appearances == undefined_appearances_range:
                print("\t\t- OK UNDEFINED appearances match")
            else:
                print("\t\t- ERROR UNDEFINED appearances do not match")
        elif is_undefined and not is_undefined_range:
            print("ERROR: undefined event appearances and not associated range")
        elif not is_undefined and is_undefined_range:
            print("ERROR: undefined event range appearances and not associated event 0")
        else:
            pass
    return report


def __filter_event_type__(trace_events, event_type):
    """
    Looks for the events in the trace matching the event type

    :param trace_events: Events found in the trace (filtered by family).
    :param event_type: Event type to filter.
    :return: Filtered trace
    """
    filtered = []
    for line in trace_events:
        if line[0] == event_type:
            filtered.append(line)
    return filtered


def __accumulate_events__(trace_events, events):
    """
    Accumulates the trace events. Calculates the amount of appearances.

    :param trace_events: Events found in the trace (filtered by family).
    :param events: Event ids to accumulate
    :return: Accumulated trace
    """
    accumulated = {}
    for line in trace_events:
        event = line[1]
        if event in events:
            if event in accumulated:
                accumulated[event] += 1
            else:
                accumulated[event] = 1
    return accumulated


def __accumulate_range__(trace_events):
    """
    Removes the type and creates a list of the events that appeared.
    Does NOT include 0.

    :param trace_events: Events found in the trace (filtered by type).
    :return: Accumulated trace
    """
    accumulated = []
    for line in trace_events:
        event = line[1]
        if event != 0:
            accumulated.append(event)
    return accumulated


def main(expected_events_file, trace_file):
    print("############################")
    print("### STARTING TRACE CHECK ###")
    print("############################")
    ok = True
    report = []
    families, event_definitions = parse_file(expected_events_file)
    trace_events = parse_trace(trace_file, families)
    # Check that the events belong to families, and if there are new
    report += check_families(trace_events, families, event_definitions)
    # Check that the events found match the defined, and if they are new
    report += check_events(trace_events, event_definitions)
    # Check each rule looking for event appearance mismatches
    report += check_rules(trace_events, event_definitions)

    if report:
        print("----------------------------")
        print("- REPORT: ")
        i = 0
        for message in report:
            print("MESSAGE %d: %s" % (i, message))
            i += 1
            ok = False
    print("----------------------------")
    print("Finished trace check!")
    if ok:
        print("- SUCCESS!")
    else:
        print("- FAILED! ERRORS FOUND. Please check the report.")
    print("----------------------------")
    return ok


if __name__ == "__main__":
    ok = main(sys.argv[1], sys.argv[2])
    if not ok:
        exit(1)
