from melius.libraries.Constants import FileTypes


def test_getFileTypeByExt(mocker):
    assert FileTypes.getFileTypeByExt('test.parquet') == FileTypes.Parquet
    assert FileTypes.getFileTypeByExt('test.csv') == FileTypes.CSV
    assert FileTypes.getFileTypeByExt('test.tsv') == FileTypes.TSV
    assert FileTypes.getFileTypeByExt('test.json') == FileTypes.JSON
    assert FileTypes.getFileTypeByExt('test.txt') == FileTypes.Unknown


def test_separator_of(mocker):
    assert FileTypes.separator_of(FileTypes.Parquet) is None
    assert FileTypes.separator_of(FileTypes.CSV) == ','
    assert FileTypes.separator_of(FileTypes.TSV) == "\t"
