/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.terminal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.function.Supplier;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.neo4j.shell.commands.CommandHelper.CommandFactoryHelper;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.CypherLanguageService;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.printer.Printer;
import org.neo4j.util.VisibleForTesting;

/**
 * Builder for CypherShellTerminals
 */
public class CypherShellTerminalBuilder {
    private static final Logger log = Logger.create();
    private Printer printer;
    private OutputStream out;
    private InputStream in;
    private boolean isInteractive = true;
    private boolean dumb;
    private ParameterService parameters;
    private Supplier<SimplePrompt> simplePromptSupplier = SimplePrompt::defaultPrompt;
    private boolean enableCypherCompletion = false;

    /** if enabled is true, this is an interactive terminal that supports user input */
    public CypherShellTerminalBuilder interactive(boolean isInteractive) {
        this.isInteractive = isInteractive;
        return this;
    }

    public CypherShellTerminalBuilder logger(Printer printer) {
        this.printer = printer;
        return this;
    }

    public CypherShellTerminalBuilder parameters(ParameterService parameters) {
        this.parameters = parameters;
        return this;
    }

    /** Set explicit streams, for testing purposes */
    @VisibleForTesting
    public CypherShellTerminalBuilder streams(InputStream in, OutputStream out) {
        this.in = in;
        this.out = out;
        return this;
    }

    /** Create a dumb terminal, for testing purposes */
    @VisibleForTesting
    public CypherShellTerminalBuilder dumb() {
        this.dumb = true;
        return this;
    }

    @VisibleForTesting
    public CypherShellTerminalBuilder simplePromptSupplier(Supplier<SimplePrompt> simplePromptSupplier) {
        this.simplePromptSupplier = simplePromptSupplier;
        return this;
    }

    public CypherShellTerminalBuilder enableCypherCompletion(boolean enable) {
        this.enableCypherCompletion = enable;
        return this;
    }

    public CypherShellTerminal build() {
        assert printer != null;

        try {
            return isInteractive ? buildJlineBasedTerminal() : nonInteractiveTerminal();
        } catch (IOException e) {
            log.warn("Fallback to non-interactive mode", e);
            if (isInteractive) {
                printer.printError("Failed to create interactive terminal, fallback to non-interactive mode");
            }
            return nonInteractiveTerminal();
        }
    }

    private CypherShellTerminal nonInteractiveTerminal() {
        return new WriteOnlyCypherShellTerminal(out != null ? new PrintStream(out) : System.out);
    }

    public CypherShellTerminal buildJlineBasedTerminal() throws IOException {
        var jLineTerminal = TerminalBuilder.builder();

        jLineTerminal.nativeSignals(true);
        jLineTerminal.paused(true); // Needed for SimplePrompt to work.

        if (in != null) {
            jLineTerminal.streams(in, out);
        }

        if (dumb) {
            var attributes = new Attributes();
            attributes.setLocalFlag(Attributes.LocalFlag.ECHO, false);
            jLineTerminal
                    .jna(false)
                    .jansi(false); // Certain environments (osx) can't handle jna/jansi mode when running tests in maven
            jLineTerminal.dumb(true).type(Terminal.TYPE_DUMB).attributes(attributes);
        }

        var cypherLangService = CypherLanguageService.get();

        var reader = LineReaderBuilder.builder()
                .terminal(jLineTerminal.build())
                .parser(new StatementJlineParser(new ShellStatementParser(), cypherLangService))
                .completer(new JlineCompleter(
                        new CommandFactoryHelper(), cypherLangService, parameters, enableCypherCompletion))
                .history(new DefaultHistory()) // The default history is in-memory until we set history file variable
                .expander(new JlineTerminal.EmptyExpander())
                .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true) // Disable '!' history expansion
                .option(LineReader.Option.DISABLE_HIGHLIGHTER, true)
                .option(LineReader.Option.CASE_INSENSITIVE, true)
                .appName("Cypher Shell")
                .build();

        return new JlineTerminal(reader, isInteractive, printer, simplePromptSupplier);
    }

    public static CypherShellTerminalBuilder terminalBuilder() {
        return new CypherShellTerminalBuilder();
    }
}
