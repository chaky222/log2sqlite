class Log2SQLite::PErrors
  @errs : Hash(String, Array(String)) = {} of String => Array(String)

  def initialize()
  end

  def field_with_err?(field_name : Symbol) : Bool
    result = false
    @errs.each { |k, v| result = true if k == "#{ field_name }" }
    result
  end

  def full_html(selected_fields : Array(String) = [] of String, ignore_field_name : Bool = false) : String
    errs = selected_fields.any? ? @errs.select { |k, v| selected_fields.includes?(k) } : @errs
    errs.map { |k, v| "<span class='err'>#{ ignore_field_name ? "" : HTML.escape(k) + ": " }" + v.map { |s| "<span class='err_item'>#{ s }</span>" }.join(' ') + "</span>" }.join("<br>")
  end

  def to_s : String
    @err_str.to_json
  end

  def any? : Bool
    !empty?
  end

  def empty? : Bool
    @errs.empty?
  end

  def push(field_name : String, err_str : String) : Bool
    @errs[field_name] = [] of String unless @errs[field_name]?
    @errs[field_name] << err_str
    true
  end
end
