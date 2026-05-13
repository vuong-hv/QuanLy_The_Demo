(function () {
    function digitsOnly(value) {
        return String(value || "").replace(/\D/g, "").replace(/^0+(?=\d)/, "");
    }

    function formatDigits(digits) {
        return String(digits || "").replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }

    function countDigitsBefore(value, caret) {
        return String(value || "").slice(0, caret).replace(/\D/g, "").length;
    }

    function caretFromDigitCount(value, digitCount) {
        if (digitCount <= 0) return 0;
        let seen = 0;
        for (let index = 0; index < value.length; index += 1) {
            if (/\d/.test(value[index])) seen += 1;
            if (seen >= digitCount) return index + 1;
        }
        return value.length;
    }

    function formatInput(input) {
        if (!input) return "";

        const previousValue = input.value;
        const previousCaret = input.selectionStart == null ? previousValue.length : input.selectionStart;
        const previousDigitsBeforeCaret = countDigitsBefore(previousValue, previousCaret);
        const digits = digitsOnly(previousValue);
        const nextValue = digits ? formatDigits(digits) : "";

        input.value = nextValue;

        if (document.activeElement === input && input.setSelectionRange) {
            const nextCaret = caretFromDigitCount(nextValue, Math.min(previousDigitsBeforeCaret, digits.length));
            requestAnimationFrame(function () {
                input.setSelectionRange(nextCaret, nextCaret);
            });
        }

        return nextValue;
    }

    function parse(value) {
        const digits = digitsOnly(value);
        return digits ? Number(digits) : 0;
    }

    window.MoneyInput = {
        digitsOnly: digitsOnly,
        formatDigits: formatDigits,
        formatInput: formatInput,
        parse: parse,
    };

    window.formatCur = formatInput;
}());
