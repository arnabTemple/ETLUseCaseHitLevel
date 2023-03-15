from SimplePipeline import ProcessHitLevelFile


def main():
    input_file = "resources/inputFiles/data.tsv"
    output_base_path = "resources/outputFiles"
    etl = ProcessHitLevelFile.ProcessHitLevelFile(input_file)
    etl.process()
    etl.output(output_base_path)


if __name__ == '__main__':
    main()
