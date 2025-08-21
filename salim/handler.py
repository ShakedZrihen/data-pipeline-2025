from extractProcess.lambda_handle import lambda_handler  # noqa: F401

if __name__ == "__main__":
    from extractProcess.poller import run
    run()
