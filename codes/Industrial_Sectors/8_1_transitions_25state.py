from subroutines.transition_outputs import run_transition_outputs


def run(output_base=None):
    return run_transition_outputs(
        state_count=25,
        by_period=False,
        output_base=output_base,
    )


if __name__ == "__main__":
    run()
