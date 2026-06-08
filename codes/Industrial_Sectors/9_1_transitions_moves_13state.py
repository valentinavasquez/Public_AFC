from subroutines.transition_moves_outputs import run_transition_moves_outputs


def run(output_base=None):
    return run_transition_moves_outputs(
        state_count=13,
        by_period=False,
        output_base=output_base,
    )


if __name__ == "__main__":
    run()
