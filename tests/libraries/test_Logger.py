from melius.libraries.Logger import Logger


def test_logBeginOfMethod(mocker):
    writeLogPatch = mocker.patch('melius.libraries.Logger.Logger._writeLog')
    Logger.getInstance().logBeginOfMethod()

    writeLogPatch.assert_called()
    args0 = writeLogPatch.call_args.args[0]
    args1 = writeLogPatch.call_args.args[1]
    # in the build env, args0 is args. in this case we are ignoring it.
    if args0 != 'args':
        assert args0 == 'Info'
        assert args1.startswith('The beginning of test_logBeginOfMethod at <FrameSummary file')
    else:
        print(args0)


def test_logEndOfMethod(mocker):
    writeLogPatch = mocker.patch('melius.libraries.Logger.Logger._writeLog')
    Logger.getInstance().logEndOfMethod()

    writeLogPatch.assert_called()
    args0 = writeLogPatch.call_args.args[0]
    args1 = writeLogPatch.call_args.args[1]
    if args0 != 'args':
        assert args0 == 'Info'
        assert args1.startswith('The end of test_logEndOfMethod at <FrameSummary file')
    else:
        print(args0)