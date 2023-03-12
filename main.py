from SimplePipeline import ProcessHitLevelFile


def main():
    input_file = "resources/inputFiles/data_modified.tsv"
    etl = ProcessHitLevelFile.ProcessHitLevelFile(input_file)
    etl.process()
    etl.output()


if __name__ == '__main__':
    main()
